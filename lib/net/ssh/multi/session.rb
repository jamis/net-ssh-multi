require 'net/ssh/gateway'
require 'net/ssh/multi/server'
require 'net/ssh/multi/channel'

module Net; module SSH; module Multi
  # Represents a collection of connections to various servers. It provides an
  # interface for organizing the connections (#group), as well as a way to
  # scope commands to a subset of all connections (#with). You can also provide
  # a default gateway connection that servers should use when connecting
  # (#via). It exposes an interface similar to Net::SSH::Connection::Session
  # for opening SSH channels and executing commands, allowing for these
  # operations to be done in parallel across multiple connections.
  #
  #   Net::SSH::Multi.start do |session|
  #     # access servers via a gateway
  #     session.via 'gateway', 'gateway-user'
  # 
  #     # define the servers we want to use
  #     session.use 'host1', 'user1'
  #     session.use 'host2', 'user2'
  # 
  #     # define servers in groups for more granular access
  #     session.group :app do
  #       session.use 'app1', 'user'
  #       session.use 'app2', 'user'
  #     end
  # 
  #     # execute commands
  #     session.exec "uptime"
  # 
  #     # execute commands on a subset of servers
  #     session.with(:app) { session.exec "hostname" }
  # 
  #     # run the aggregated event loop
  #     session.loop
  #   end
  #
  # Note that connections are established lazily, as soon as they are needed.
  # You can force the connections to be opened immediately, though, using the
  # #connect! method.
  class Session
    # The list of Net::SSH::Multi::Server definitions managed by this session.
    attr_reader :servers

    # The default Net::SSH::Gateway instance to use to connect to the servers.
    # If +nil+, no default gateway will be used.
    attr_reader :default_gateway

    # The hash of group definitions, mapping each group name to the list of
    # corresponding Net::SSH::Multi::Server definitions.
    attr_reader :groups

    # The list of "open" groups, which will receive subsequent server definitions.
    # See #use and #group.
    attr_reader :open_groups #:nodoc:

    # The list of "active" groups, which will be used to restrict subsequent
    # commands. This is actually a Hash, mapping group names to their corresponding
    # constraints (see #with).
    attr_reader :active_groups #:nodoc:

    # Creates a new Net::SSH::Multi::Session instance. Initially, it contains
    # no server definitions, no group definitions, and no default gateway.
    def initialize
      @servers = []
      @groups = {}
      @gateway = nil
      @active_groups = {}
      @open_groups = []
    end

    # At its simplest, this associates a named group with a server definition.
    # It can be used in either of two ways:
    #
    # First, you can use it to associate a group (or array of groups) with a
    # server definition (or array of server definitions). The server definitions
    # must already exist in the #servers array (typically by calling #use):
    #
    #   server1 = session.use('host1', 'user1')
    #   server2 = session.use('host2', 'user2')
    #   session.group :app => server1, :web => server2
    #   session.group :staging => [server1, server2]
    #   session.group %w(xen linux) => server2
    #   session.group %w(rackspace backup) => [server1, server2]
    #
    # Secondly, instead of a mapping of groups to servers, you can just
    # provide a list of group names, and then a block. Inside the block, any
    # calls to #use will automatically associate the new server definition with
    # those groups. You can nest #group calls, too, which will aggregate the
    # group definitions.
    #
    #   session.group :rackspace, :backup do
    #     session.use 'host1', 'user1'
    #     session.group :xen do
    #       session.use 'host2', 'user2'
    #     end
    #   end
    def group(*args)
      mapping = args.last.is_a?(Hash) ? args.pop : {}

      if mapping.any? && block_given?
        raise ArgumentError, "must provide group mapping OR block, not both"
      elsif block_given?
        begin
          saved_groups = open_groups.dup
          open_groups.concat(args.map { |a| a.to_sym }).uniq!
          yield self if block_given?
        ensure
          open_groups.replace(saved_groups)
        end
      else
        mapping.each do |key, value|
          (open_groups + Array(key)).uniq.each do |grp|
            (groups[grp.to_sym] ||= []).concat(Array(value)).uniq!
          end
        end
      end
    end

    # Sets up a default gateway to use when establishing connections to servers.
    # Note that any servers defined prior to this invocation will not use the
    # default gateway; it only affects servers defined subsequently.
    #
    #   session.via 'gateway.host', 'user'
    #
    # You may override the default gateway on a per-server basis by passing the
    # :via key to the #use method; see #use for details.
    def via(host, user, options={})
      @default_gateway = Net::SSH::Gateway.new(host, user, options)
      self
    end

    # Defines a new server definition, to be managed by this session. The
    # server is at the given +host+, and will be connected to as the given
    # +user+. The other options are passed as-is to the Net::SSH session
    # constructor.
    #
    # If a default gateway has been specified previously (with #via) it will
    # be passed to the new server definition. You can override this by passing
    # a different Net::SSH::Gateway instance (or +nil+) with the :via key in
    # the +options+.
    #
    #   session.use 'host', 'user'
    #   session.use 'host2', 'user2', :via => nil
    #   session.use 'host3', 'user3', :via => Net::SSH::Gateway.new('gateway.host', 'user')
    def use(host, user, options={})
      server = Server.new(host, user, {:via => default_gateway}.merge(options))
      exists = servers.index(server)
      if exists
        server = servers[exists]
      else
        servers << server
        group [] => server
      end
      server
    end

    # Restricts the set of servers that will be targeted by commands within
    # the associated block. It can be used in either of two ways (or both ways
    # used together).
    #
    # First, you can simply specify a list of group names. All servers in all
    # named groups will be the target of the commands. (Nested calls to #with
    # are cumulative.)
    #
    #   # execute 'hostname' on all servers in the :app group, and 'uptime'
    #   # on all servers in either :app or :db.
    #   session.with(:app) do
    #     session.exec('hostname')
    #     session.with(:db) do
    #       session.exec('uptime')
    #     end
    #   end
    #
    # Secondly, you can specify a hash with group names as keys, and property
    # constraints as the values. These property constraints are either "only"
    # constraints (which restrict the set of servers to "only" those that match
    # the given properties) or "except" constraints (which restrict the set of
    # servers to those whose properties do _not_ match). Properties are described
    # when the server is defined (via the :properties key):
    #
    #   session.group :db do
    #     session.use 'dbmain', 'user', :properties => { :primary => true }
    #     session.use 'dbslave', 'user2'
    #     session.use 'dbslve2', 'user2'
    #   end
    #
    #   # execute the given rake task ONLY on the servers in the :db group
    #   # which have the :primary property set to true.
    #   session.with :db => { :only => { :primary => true } } do
    #     session.exec "rake db:migrate"
    #   end
    #
    # You can, naturally, combine these methods:
    #
    #   # all servers in :app and :web, and all servers in :db with the
    #   # :primary property set to true
    #   session.with :app, :web, :db => { :only => { :primary => true } } do
    #     # ...
    #   end
    def with(*groups)
      saved_groups = active_groups.dup

      new_map = groups.inject({}) do |map, group|
        if group.is_a?(Hash)
          group.each do |gr, value|
            raise ArgumentError, "the value for any group must be a Hash" unless value.is_a?(Hash)
            bad_keys = value.keys - [:only, :except]
            raise ArgumentError, "unknown constraint(s): #{bad_keys.inspect}" unless bad_keys.empty?
            map[gr] = (active_groups[gr] || {}).merge(value)
          end
        else
          map[group] = active_groups[group] || {}
        end
        map
      end

      active_groups.update(new_map)
      yield self
    ensure
      active_groups.replace(saved_groups)
    end

    # Works as #with, but for specific servers rather than groups. In other
    # words, you can use this to restrict actions within the block to only
    # a specific list of servers. It works by creating an ad-hoc group, adding
    # the servers to that group, and then making that group the only active
    # group. (Note that because of this, you cannot nest #on within #with,
    # though you could nest #with inside of #on.)
    #
    #   srv = session.use('host', 'user')
    #   # ...
    #   session.on(srv) do
    #     session.exec('hostname')
    #   end
    def on(*servers)
      adhoc_group = "adhoc_group_#{servers.hash}_#{rand(0xffffffff)}".to_sym
      group(adhoc_group => servers)
      saved_groups = active_groups.dup
      active_groups.replace(adhoc_group => {})
      yield self
    ensure
      active_groups.replace(saved_groups) if saved_groups
      groups.delete(adhoc_group)
    end

    # Returns the list of Net::SSH sessions for all servers that match the
    # current scope (e.g., the groups or servers named in the outer #with or
    # #on calls). If any servers have not yet been connected to, this will
    # block until the connections have been made.
    def active_sessions
      list = if active_groups.empty?
        servers
      else
        active_groups.inject([]) do |list, (group, properties)|
          servers = groups[group].select do |server|
            (properties[:only] || {}).all? { |prop, value| server[prop] == value } &&
            !(properties[:except] || {}).any? { |prop, value| server[prop] == value }
          end
          list.concat(servers)
        end
      end

      list.uniq!
      threads = list.map { |server| Thread.new { server.session(true) } if server.session.nil? }
      threads.each { |thread| thread.join if thread }

      list.map { |server| server.session }
    end

    # Connections are normally established lazily, as soon as they are needed.
    # This method forces all servers selected by the current scope to connect,
    # if they have not yet been connected.
    def connect!
      active_sessions
      self
    end

    # Closes the multi-session by shutting down all open server sessions, and
    # the default gateway (if one was specified using #via). Note that other
    # gateway connections (e.g., those passed to #use directly) will _not_ be
    # closed by this method, and must be managed externally.
    def close
      servers.each { |server| server.close_channels }
      loop(0) { busy?(true) }
      servers.each { |server| server.close }
      default_gateway.shutdown! if default_gateway
    end

    # Returns +true+ if any server has an open SSH session that is currently
    # processing any channels. If +include_invisible+ is +false+ (the default)
    # then invisible channels (such as those created by port forwarding) will
    # not be counted; otherwise, they will be.
    def busy?(include_invisible=false)
      servers.any? { |server| server.busy?(include_invisible) }
    end

    alias :loop_forever :loop

    # Run the aggregated event loop for all open server sessions, until the given
    # block returns +false+. If no block is given, the loop will run for as
    # long as #busy? returns +true+ (in other words, for as long as there are
    # any (non-invisible) channels open).
    def loop(wait=nil, &block)
      running = block || Proc.new { |c| busy? }
      loop_forever { break unless process(wait, &running) }
    end

    # Run a single iteration of the aggregated event loop for all open server
    # sessions. The +wait+ parameter indicates how long to wait for an event
    # to appear on any of the different sessions; +nil+ (the default) means
    # "wait forever". If the block is given, then it will be used to determine
    # whether #process returns +true+ (the block did not return +false+), or
    # +false+ (the block returned +false+).
    def process(wait=nil, &block)
      return false unless preprocess(&block)

      readers = servers.map { |s| s.readers }.flatten
      writers = servers.map { |s| s.writers }.flatten

      readers, writers, = IO.select(readers, writers, nil, wait)

      return postprocess(readers, writers)
    end

    # Sends a global request to all active sessions (see #active_sessions).
    # This can be used to (e.g.) ping the remote servers to prevent them from
    # timing out.
    #
    #   session.send_global_request("keep-alive@openssh.com")
    #
    # If a block is given, it will be invoked when the server responds, with
    # two arguments: the Net::SSH connection that is responding, and a boolean
    # indicating whether the request succeeded or not.
    def send_global_request(type, *extra, &callback)
      active_sessions.each { |ssh| ssh.send_global_request(type, *extra, &callback) }
      self
    end

    # Asks all active sessions (see #active_sessions) to open a new channel.
    # When each server responds, the +on_confirm+ block will be invoked with
    # a single argument, the channel object for that server. This means that
    # the block will be invoked one time for each active session.
    #
    # All new channels will be collected and returned, aggregated into a new
    # Net::SSH::Multi::Channel instance.
    #
    # Note that the channels are "enhanced" slightly--they have two properties
    # set on them automatically, to make dealing with them in a multi-session
    # environment slightly easier:
    #
    # * :server => the Net::SSH::Multi::Server instance that spawned the channel
    # * :host => the host name of the server
    #
    # Having access to these things lets you more easily report which host
    # (e.g.) data was received from:
    #
    #   session.open_channel do |channel|
    #     channel.exec "command" do |ch, success|
    #       ch.on_data do |ch, data|
    #         puts "got data #{data} from #{ch[:host]}"
    #       end
    #     end
    #   end
    def open_channel(type="session", *extra, &on_confirm)
      channels = active_sessions.map do |ssh|
        ssh.open_channel(type, *extra) do |c|
          c[:server] = ssh[:server]
          c[:host] = ssh[:server].host
          on_confirm[c] if on_confirm
        end
      end
      Multi::Channel.new(self, channels)
    end

    # A convenience method for executing a command on multiple hosts and
    # either displaying or capturing the output. It opens a channel on all
    # active sessions (see #open_channel and #active_sessions), and then
    # executes a command on each channel (Net::SSH::Connection::Channel#exec).
    #
    # If a block is given, it will be invoked whenever data is received across
    # the channel, with three arguments: the channel object, a symbol identifying
    # which output stream the data was received on (+:stdout+ or +:stderr+)
    # and a string containing the data that was received:
    #
    #   session.exec("command") do |ch, stream, data|
    #     puts "[#{ch[:host]} : #{stream}] #{data}"
    #   end
    #
    # If no block is given, all output will be written to +$stdout+ or
    # +$stderr+, as appropriate.
    #
    # Note that #exec will also capture the exit status of the process in the
    # +:exit_status+ property of each channel. Since #exec returns all of the
    # channels in a Net::SSH::Multi::Channel object, you can check for the
    # exit status like this:
    #
    #   channel = session.exec("command") { ... }
    #   channel.wait
    #
    #   if channel.any? { |c| c[:exit_status] != 0 }
    #     puts "executing failed on at least one host!"
    #   end
    def exec(command, &block)
      open_channel do |channel|
        channel.exec(command) do |ch, success|
          raise "could not execute command: #{command.inspect} (#{ch[:host]})" unless success

          channel.on_data do |ch, data|
            if block
              block.call(ch, :stdout, data)
            else
              data.chomp.each_line do |line|
                $stdout.puts("[#{ch[:host]}] #{line}")
              end
            end
          end

          channel.on_extended_data do |ch, type, data|
            if block
              block.call(ch, :stderr, data)
            else
              data.chomp.each_line do |line|
                $stderr.puts("[#{ch[:host]}] #{line}")
              end
            end
          end

          channel.on_request("exit-status") do |ch, data|
            ch[:exit_status] = data.read_long
          end
        end
      end
    end

    # Runs the preprocess stage on all servers. Returns false if the block
    # returns false, and true if there either is no block, or it returns true.
    # This is called as part of the #process method.
    def preprocess(&block) #:nodoc:
      return false if block && !block[self]
      servers.each { |server| server.preprocess }
      block.nil? || block[self]
    end

    # Runs the postprocess stage on all servers. Always returns true. This is
    # called as part of the #process method.
    def postprocess(readers, writers) #:nodoc:
      servers.each { |server| server.postprocess(readers, writers) }
      true
    end
  end
end; end; end