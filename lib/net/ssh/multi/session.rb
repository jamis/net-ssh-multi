require 'net/ssh/gateway'
require 'net/ssh/multi/server'
require 'net/ssh/multi/channel'

module Net; module SSH; module Multi
  class Session
    attr_reader :servers
    attr_reader :default_gateway
    attr_reader :groups

    attr_reader :open_groups
    attr_reader :active_groups

    def initialize
      @servers = []
      @groups = {}
      @gateway = nil
      @active_groups = {}
      @open_groups = []
    end

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

    def via(host, user, options={})
      @default_gateway = Net::SSH::Gateway.new(host, user, options)
      self
    end

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

      sessions_for(list.uniq)
    end

    def connect!
      active_sessions
      self
    end

    def close
      servers.each { |server| server.close_channels }
      loop(0) { busy?(true) }
      servers.each { |server| server.close }
      default_gateway.shutdown! if default_gateway
    end

    def busy?(include_invisible=false)
      servers.any? { |server| server.busy?(include_invisible) }
    end

    alias :loop_forever :loop

    def loop(wait=nil, &block)
      running = block || Proc.new { |c| busy? }
      loop_forever { break unless process(wait, &running) }
    end

    def preprocess(&block)
      return false if block && !block[self]
      servers.each { |server| server.preprocess }
      block.nil? || block[self]
    end

    def postprocess(readers, writers)
      servers.each { |server| server.postprocess(readers, writers) }
      true
    end

    def process(wait=nil, &block)
      return false unless preprocess(&block)

      readers = servers.map { |s| s.readers }.flatten
      writers = servers.map { |s| s.writers }.flatten

      readers, writers, = IO.select(readers, writers, nil, wait)

      return postprocess(readers, writers)
    end

    def send_global_request(type, *extra, &callback)
      active_sessions.each { |ssh| ssh.send_global_request(type, *extra, &callback) }
      self
    end

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

    private

      def sessions_for(servers)
        threads = servers.map { |server| Thread.new { server.session(true) } if server.session.nil? }
        threads.each { |thread| thread.join if thread }
        servers.map { |server| server.session }
      end
  end
end; end; end