require 'thread'
require 'net/ssh'
require 'net/ssh/gateway'
require 'net/ssh/multi/channel'

module Net; module SSH; module Multi
  class Session
    attr_reader :connections
    attr_reader :gateway
    attr_reader :groups

    def initialize
      @connections = []
      @groups = {}
      @gateway = nil
      @connections_mutex = Mutex.new
      @groups_mutex = Mutex.new
      @active_groups = []
    end

    def group(*args)
      mapping = args.last.is_a?(Hash) ? args.pop : {}

      begin
        saved_groups = active_groups.dup
        active_groups.concat(args.map { |a| a.to_sym }).uniq!

        mapping.each do |key, value|
          (active_groups + Array(key)).uniq.each do |grp|
            (groups[grp.to_sym] ||= []).concat(Array(value))
          end
        end

        yield self if block_given?
      ensure
        active_groups.replace(saved_groups)
      end
    end

    def via(*args)
      if connection_specification?(args)
        @gateway = Net::SSH::Gateway.new(*args)
      elsif args.length == 1
        @gateway = args.first
      else
        raise ArgumentError, "expected either a connection specification or a Net::SSH::Gateway instance"
      end
      self
    end

    def use(*list)
      connections.concat(list.each { |c| c[:host] = c.host })
      group(active_groups => list)
      self
    end

    def connect(*args)
      if connection_specification?(args)
        establish_connection(*args)
      elsif args.any?
        raise ArgumentError, "expected either a connection specification or a block"
      end

      if block_given?
        collector = Collector.new
        yield collector

        threads = collector.specifications.map do |spec|
          Thread.new { establish_connection(spec.host, spec.user, spec.options, spec.groups) }
        end

        threads.each { |t| t.join }
      end

      self
    end

    def with(*groups)
      saved_groups = active_groups.dup
      active_groups.concat(groups).uniq!
      yield self
    ensure
      active_groups.replace(saved_groups)
    end

    def active_connections
      if active_groups.empty?
        connections
      else
        active_groups.map { |group| groups[group] }.flatten.uniq
      end
    end

    def close
      connections.each { |connection| connection.channels.each { |id, channel| channel.close } }
      loop(0) { busy?(true) }
      connections.each { |connection| connection.transport.close }
      gateway.shutdown! if gateway
    end

    def busy?(include_invisible=false)
      @connections.any? { |connection| connection.busy?(include_invisible) }
    end

    alias :loop_forever :loop

    def loop(wait=nil, &block)
      running = block || Proc.new { |c| busy? }
      loop_forever { break unless process(wait, &running) }
    end

    def process(wait=nil, &block)
      @connections.each { |c| return false unless c.preprocess(&block) }

      writers_by_connection, readers_by_connection = {}, {}

      writers = @connections.map do |c|
        c.listeners.keys.select do |w|
          writers_by_connection[c] ||= []
          writers_by_connection[c] << w
          w.respond_to?(:pending_write?) && w.pending_write?
        end
      end.flatten

      readers = @connections.map { |c| readers_by_connection[c] = c.listeners.keys }.flatten

      readers, writers = IO.select(readers, writers, nil, wait)

      @connections.each do |c|
        readers_for_this = readers_by_connection[c] & (readers || [])
        writers_for_this = writers_by_connection[c] & (writers || [])
        return false unless c.postprocess(readers_for_this, writers_for_this)
      end

      return true
    end

    def send_global_request(type, *extra, &callback)
      active_connections.each { |connection| connection.send_global_request(type, *extra, &callback) }
      self
    end

    def open_channel(type="session", *extra, &on_confirm)
      channels = active_connections.map do |connection|
        channel = connection.open_channel(type, *extra, &on_confirm)
        channel[:host] = connection[:host]
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
        ch[:result][ch.connection[:host]] ||= ""
        ch[:result][ch.connection[:host]] << data
      end

      channel = exec(command, &block)
      channel.wait

      return channel[:result]
    end

    private

      def active_groups
        @active_groups
      end

      def connection_specification?(args)
        args.length == 2 || (args.length == 3 && args.last.is_a?(Hash))
      end

      def establish_connection(host, user, options, groups=[])
        connection = gateway ? gateway.ssh(host, user, options) :
          Net::SSH.start(host, user, options)
        connection[:host] = host
        @connections_mutex.synchronize { connections.push(connection) }
        @groups_mutex.synchronize { group((active_groups + groups).uniq => connection) }
        return connection
      rescue Net::SSH::AuthenticationFailed => error
        error.message << "@#{host}"
        raise
      end

      class Collector
        class Specification
          attr_reader :host, :user, :options
          attr_reader :groups

          def initialize(host, user, options, groups)
            @host, @user, @options = host, user, options.dup
            @groups = groups.dup
          end
        end

        attr_reader :specifications

        def initialize
          @specifications = []
          @active_groups = []
        end

        def to(host, user, options={})
          @specifications << Specification.new(host, user, options, @active_groups)
          @specifications.length - 1
        end

        def group(*args)
          mapping = args.last.is_a?(Hash) ? args.pop : {}

          begin
            saved_groups = @active_groups.dup
            @active_groups.concat(args.map { |a| a.to_sym }).uniq!

            mapping.each do |key, value|
              groups = (Array(key).map { |v| v.to_sym } + @active_groups).uniq

              Array(value).each do |id|
                @specifications[id].groups.concat(groups).uniq!
              end
            end

            yield self if block_given?
          ensure
            @active_groups.replace(saved_groups)
          end
        end
      end
  end
end; end; end