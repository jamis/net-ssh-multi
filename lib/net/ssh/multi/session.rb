require 'thread'
require 'net/ssh/gateway'
require 'net/ssh/multi/server'
require 'net/ssh/multi/channel'

module Net; module SSH; module Multi
  class Session
    attr_reader :servers
    attr_reader :default_gateway
    attr_reader :groups

    def initialize
      @servers = []
      @groups = {}
      @gateway = nil
      @connections_mutex = Mutex.new
      @groups_mutex = Mutex.new
      @active_groups = []
    end

    def group(*args)
      mapping = args.last.is_a?(Hash) ? args.pop : {}

      if mapping.any? && block_given?
        raise ArgumentError, "must provide group mapping OR block, not both"
      elsif block_given?
        begin
          saved_groups = active_groups.dup
          active_groups.concat(args.map { |a| a.to_sym }).uniq!
          yield self if block_given?
        ensure
          active_groups.replace(saved_groups)
        end
      else
        mapping.each do |key, value|
          (active_groups + Array(key)).uniq.each do |grp|
            (groups[grp.to_sym] ||= []).concat(Array(value))
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
      unless servers.include?(server)
        servers << server
        group [] => server
      end
      server
    end

    def with(*groups)
      saved_groups = active_groups.dup
      active_groups.concat(groups).uniq!
      yield self
    ensure
      active_groups.replace(saved_groups)
    end

    def active_sessions
      list = if active_groups.empty?
        servers
      else
        active_groups.map { |group| groups[group] }.flatten.uniq
      end

      sessions_for(list)
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

    def process(wait=nil, &block)
      return false if servers.any? { |server| !server.preprocess(&block) }

      readers = servers.map { |s| s.readers }.flatten
      writers = servers.map { |s| s.writers }.flatten

      readers, writers, = IO.select(readers, writers, nil, wait)

      return servers.all? { |server| server.postprocess(readers, writers) }
    end

    def send_global_request(type, *extra, &callback)
      active_sessions.each { |ssh| ssh.send_global_request(type, *extra, &callback) }
      self
    end

    def open_channel(type="session", *extra, &on_confirm)
      channels = active_sessions.map do |ssh|
        channel = ssh.open_channel(type, *extra, &on_confirm)
        channel[:server] = ssh[:server]
        channel[:host] = ssh[:server].host
        channel
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

    def exec!(command, &block)
      block ||= Proc.new do |ch, type, data|
        ch[:result] ||= {}
        ch[:result][ch[:server]] ||= ""
        ch[:result][ch[:server]] << data
      end

      channel = exec(command, &block)
      channel.wait

      return channel[:result]
    end

    private

      def active_groups
        @active_groups
      end

      def sessions_for(servers)
        threads = servers.map { |server| Thread.new { server.session(true) } }
        threads.each { |thread| thread.join }
        servers.map { |server| server.session }
      end
  end
end; end; end