require 'thread'
require 'net/ssh'
require 'net/ssh/gateway'
require 'net/ssh/multi/channel'

module Net; module SSH; module Multi
  class Session
    attr_reader :connections
    attr_reader :gateway

    class Collector
      attr_reader :specifications

      def initialize
        @specifications = []
      end

      def to(host, user, options={})
        @specifications << [host, user, options]
        self
      end
    end

    def initialize
      @connections = []
      @gateway = nil
      @mutex = Mutex.new
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
      @connections += list.each { |c| c[:host] = c.host }
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

        mutex = Mutex.new
        threads = collector.specifications.map do |host, user, options|
          Thread.new { establish_connection(host, user, options) }
        end

        threads.each { |t| t.join }
      end

      self
    end

    def close
      connections.each { |connection| connection.channels.each { |id, channel| channel.close } }
      loop(0) { busy?(true) }
      connections.each { |connection| connection.transport.close }
      gateway.shutdown! if gateway
    end

    def busy?(include_invisible=false)
      connections.any? { |connection| connection.busy?(include_invisible) }
    end

    alias :loop_forever :loop

    def loop(wait=nil, &block)
      running = block || Proc.new { |c| busy? }
      loop_forever { break unless process(wait, &running) }
    end

    def process(wait=nil, &block)
      connections.each { |c| return false unless c.preprocess(&block) }

      writers_by_connection, readers_by_connection = {}, {}

      writers = connections.map do |c|
        c.listeners.keys.select do |w|
          writers_by_connection[c] ||= []
          writers_by_connection[c] << w
          w.respond_to?(:pending_write?) && w.pending_write?
        end
      end.flatten

      readers = connections.map { |c| readers_by_connection[c] = c.listeners.keys }.flatten

      readers, writers = IO.select(readers, writers, nil, wait)

      connections.each do |c|
        readers_for_this = readers_by_connection[c] & (readers || [])
        writers_for_this = writers_by_connection[c] & (writers || [])
        return false unless c.postprocess(readers_for_this, writers_for_this)
      end

      return true
    end

    def send_global_request(type, *extra, &callback)
      connections.each { |connection| connection.send_global_request(type, *extra, &callback) }
      self
    end

    def open_channel(type="session", *extra, &on_confirm)
      channels = connections.map do |connection|
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

    def send_message(message)
      connections.each { |connection| connection.send_message(message) }
      self
    end

    private

      def connection_specification?(args)
        args.length == 2 || (args.length == 3 && args.last.is_a?(Hash))
      end

      def establish_connection(host, user, options={})
        connection = gateway ? gateway.ssh(host, user, options) :
          Net::SSH.start(host, user, options)
        connection[:host] = host
        @mutex.synchronize { @connections.push(connection) }
        return connection
      end
  end
end; end; end