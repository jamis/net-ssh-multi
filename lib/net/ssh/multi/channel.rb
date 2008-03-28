module Net; module SSH; module Multi
  class Channel
    include Enumerable

    attr_reader :connection
    attr_reader :channels
    attr_reader :properties

    def initialize(connection, channels)
      @connection = connection
      @channels = channels
      @properties = {}
    end

    def each
      @channels.each { |channel| yield channel }
    end

    def [](key)
      @properties[key.to_sym]
    end

    def []=(key, value)
      @properties[key.to_sym] = value
    end

    def exec(command, &block)
      channels.each { |channel| channel.exec(command, &block) }
      self
    end

    def subsystem(subsystem, &block)
      channels.each { |channel| channel.subsystem(subsystem, &block) }
      self
    end

    def request_pty(opts={}, &block)
      channels.each { |channel| channel.request_pty(opts, &block) }
      self
    end

    def send_data(data)
      channels.each { |channel| channel.send_data(data) }
      self
    end

    def active?
      channels.any? { |channel| channel.active? }
    end

    def wait
      connection.loop { active? }
      self
    end

    def close
      channels.each { |channel| channel.close }
      self
    end

    def eof!
      channels.each { |channel| channel.eof! }
      self
    end

    def on_data(&block)
      channels.each { |channel| channel.on_data(&block) }
      self
    end

    def on_extended_data(&block)
      channels.each { |channel| channel.on_extended_data(&block) }
      self
    end

    def on_process(&block)
      channels.each { |channel| channel.on_process(&block) }
      self
    end

    def on_close(&block)
      channels.each { |channel| channel.on_close(&block) }
      self
    end

    def on_eof(&block)
      channels.each { |channel| channel.on_eof(&block) }
      self
    end

    def on_request(type, &block)
      channels.each { |channel| channel.on_request(type, &block) }
      self
    end
  end
end; end; end