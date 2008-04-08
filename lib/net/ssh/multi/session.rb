require 'thread'
require 'net/ssh/gateway'
require 'net/ssh/multi/server'
require 'net/ssh/multi/channel'
require 'net/ssh/multi/pending_connection'
require 'net/ssh/multi/session_actions'
require 'net/ssh/multi/subsession'

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
  #     # execute commands on all servers
  #     session.exec "uptime"
  # 
  #     # execute commands on a subset of servers
  #     session.with(:app).exec "hostname"
  # 
  #     # run the aggregated event loop
  #     session.loop
  #   end
  #
  # Note that connections are established lazily, as soon as they are needed.
  # You can force the connections to be opened immediately, though, using the
  # #connect! method.
  class Session
    include SessionActions

    # The list of Net::SSH::Multi::Server definitions managed by this session.
    attr_reader :servers

    # The default Net::SSH::Gateway instance to use to connect to the servers.
    # If +nil+, no default gateway will be used.
    attr_reader :default_gateway

    # The hash of group definitions, mapping each group name to the list of
    # corresponding Net::SSH::Multi::Server definitions.
    attr_reader :groups

    # The number of allowed concurrent connections. No more than this number
    # of sessions will be open at any given time.
    attr_accessor :concurrent_connections

    # How connection errors should be handled. This defaults to :fail, but
    # may be set to :ignore if connection errors should be ignored, or
    # :warn if connection errors should cause a warning.
    attr_accessor :on_error

    # The number of connections that are currently open.
    attr_reader :open_connections #:nodoc:

    # The list of "open" groups, which will receive subsequent server definitions.
    # See #use and #group.
    attr_reader :open_groups #:nodoc:

    # Creates a new Net::SSH::Multi::Session instance. Initially, it contains
    # no server definitions, no group definitions, and no default gateway.
    #
    # You can set the #concurrent_connections property in the options. Setting
    # it to +nil+ (the default) will cause Net::SSH::Multi to ignore any
    # concurrent connection limit and allow all defined sessions to be open
    # simultaneously. Setting it to an integer will cause Net::SSH::Multi to
    # allow no more than that number of concurrently open sessions, opening
    # subsequent sessions only when other sessions finish and close.
    #
    #   Net::SSH::Multi.start(:concurrent_connections => 10) do |session|
    #     session.use ...
    #   end
    def initialize(options={})
      @servers = []
      @groups = {}
      @gateway = nil
      @open_groups = []
      @connect_threads = []
      @on_error = :fail

      @open_connections = 0
      @pending_sessions = []
      @session_mutex = Mutex.new

      options.each { |opt, value| send("#{opt}=", value) }
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
      server = Server.new(self, host, user, {:via => default_gateway}.merge(options))
      exists = servers.index(server)
      if exists
        server = servers[exists]
      else
        servers << server
        group [] => server
      end
      server
    end

    # Returns the set of servers that match the given criteria. It can be used
    # in any (or all) of three ways.
    #
    # First, you can omit any arguments. In this case, the full list of servers
    # will be returned.
    #
    #   all = session.servers_for
    #
    # Second, you can simply specify a list of group names. All servers in all
    # named groups will be returned. If a server belongs to multiple matching
    # groups, then it will appear only once in the list (the resulting list
    # will contain only unique servers).
    #
    #   servers = session.servers_for(:app, :db)
    #
    # Last, you can specify a hash with group names as keys, and property
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
    #   # return ONLY on the servers in the :db group which have the :primary
    #   # property set to true.
    #   primary = session.servers_for(:db => { :only => { :primary => true } })
    #
    # You can, naturally, combine these methods:
    #
    #   # all servers in :app and :web, and all servers in :db with the
    #   # :primary property set to true
    #   servers = session.servers_for(:app, :web, :db => { :only => { :primary => true } })
    def servers_for(*criteria)
      if criteria.empty?
        servers
      else
        # normalize the criteria list, so that every entry is a key to a
        # criteria hash (possibly empty).
        criteria = criteria.inject({}) do |hash, entry|
          case entry
          when Hash then hash.merge(entry)
          else hash.merge(entry => {})
          end
        end

        list = criteria.inject([]) do |server_list, (group, properties)|
          raise ArgumentError, "the value for any group must be a Hash, but got a #{properties.class} for #{group.inspect}" unless properties.is_a?(Hash)
          bad_keys = properties.keys - [:only, :except]
          raise ArgumentError, "unknown constraint(s) #{bad_keys.inspect} for #{group.inspect}" unless bad_keys.empty?

          servers = (groups[group] || []).select do |server|
            (properties[:only] || {}).all? { |prop, value| server[prop] == value } &&
            !(properties[:except] || {}).any? { |prop, value| server[prop] == value }
          end
          server_list.concat(servers)
        end

        list.uniq
      end
    end

    # Returns a new Net::SSH::Multi::Subsession instance consisting of the
    # servers that meet the given criteria. If a block is given, the
    # subsession will be yielded to it. See #servers_for for a discussion of
    # how these criteria are interpreted.
    #
    #   session.with(:app).exec('hostname')
    #
    #   session.with(:app, :db => { :primary => true }) do |s|
    #     s.exec 'date'
    #     s.exec 'uptime'
    #   end
    def with(*groups)
      subsession = Subsession.new(self, servers_for(*groups))
      yield subsession if block_given?
      subsession
    end

    # Works as #with, but for specific servers rather than groups. It will
    # return a new subsession (Net::SSH::Multi::Subsession) consisting of
    # the given servers. (Note that it requires that the servers in question
    # have been created via calls to #use on this session object, or things
    # will not work quite right.) If a block is given, the new subsession
    # will also be yielded to the block.
    #
    #   srv1 = session.use('host1', 'user')
    #   srv2 = session.use('host2', 'user')
    #   # ...
    #   session.on(srv1, srv2).exec('hostname')
    def on(*servers)
      subsession = Subsession.new(self, servers)
      yield subsession if block_given?
      subsession
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
      realize_pending_connections!
      wait = @connect_threads.any? ? 0 : wait

      return false unless preprocess(&block)

      readers = servers.map { |s| s.readers }.flatten
      writers = servers.map { |s| s.writers }.flatten

      readers, writers, = IO.select(readers, writers, nil, wait)

      if readers
        return postprocess(readers, writers)
      else
        return true
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

    # Takes the #concurrent_connections property into account, and tries to
    # return a new session for the given server. If the concurrent connections
    # limit has been reached, then a Net::SSH::Multi::PendingConnection instance
    # will be returned instead, which will be realized into an actual session
    # as soon as a slot opens up.
    #
    # If +force+ is true, the concurrent_connections check is skipped and a real
    # connection is always returned.
    def next_session(server, force=false) #:nodoc:
      # don't retry a failed attempt
      return nil if server.failed?

      @session_mutex.synchronize do
        if !force && concurrent_connections && concurrent_connections <= open_connections
          connection = PendingConnection.new(server)
          @pending_sessions << connection
          return connection
        end

        @open_connections += 1
      end

      begin
        server.new_session
      rescue Exception => e
        server.fail!
        @session_mutex.synchronize { @open_connections -= 1 }

        case on_error
        when :ignore then
          # do nothing
        when :warn then
          warn("error connecting to #{server}: #{e.class} (#{e.message})")
        when Proc then
          go = catch(:go) { on_error.call(server); nil }
          case go
          when nil, :ignore then # nothing
          when :retry then retry
          when :raise then raise
          else warn "unknown 'go' command: #{go.inspect}"
          end
        else
          raise
        end

        return nil
      end
    end

    # Tells the session that the given server has closed its connection. The
    # session indicates that a new connection slot is available, which may be
    # filled by the next pending connection on the next event loop iteration.
    def server_closed(server) #:nodoc:
      @session_mutex.synchronize do
        unless @pending_sessions.delete(server.session)
          @open_connections -= 1
        end
      end
    end

    # Invoked by the event loop. If there is a concurrent_connections limit in
    # effect, this will close any non-busy sessions and try to open as many
    # new sessions as it can. It does this in threads, so that existing processing
    # can continue.
    #
    # If there is no concurrent_connections limit in effect, then this method
    # does nothing.
    def realize_pending_connections! #:nodoc:
      return unless concurrent_connections

      servers.each do |s|
        s.close if !s.busy?(true)
        s.update_session!
      end

      @connect_threads.delete_if { |t| !t.alive? }

      count = concurrent_connections ? (concurrent_connections - open_connections) : @pending_sessions.length
      count.times do
        session = @pending_sessions.pop or break
        @connect_threads << Thread.new do
          session.replace_with(next_session(session.server, true))
        end
      end
    end
  end
end; end; end