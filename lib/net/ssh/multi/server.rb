require 'net/ssh'

module Net; module SSH; module Multi
  class Server
    attr_reader :host
    attr_reader :user
    attr_reader :options
    attr_reader :gateway

    def initialize(host, user, options={})
      @host = host
      @user = user
      @options = options.dup
      @gateway = @options.delete(:via)
    end

    def [](key)
      (@options[:properties] || {})[key]
    end

    def port
      options[:port] || 22
    end

    def eql?(server)
      host == server.host &&
      user == server.user &&
      port == server.port
    end

    alias :== :eql?

    def hash
      @hash ||= [host, user, port].hash
    end

    def to_s
      @to_s ||= begin
        s = "#{user}@#{host}"
        s << ":#{options[:port]}" if options[:port]
        s
      end
    end

    def inspect
      @inspect ||= "#<%s:0x%x %s>" % [self.class.name, object_id, to_s]
    end

    def session(ensure_open=false)
      return @session if @session || !ensure_open
      @session ||= begin 
        session = if gateway
          gateway.ssh(host, user, options)
        else
          Net::SSH.start(host, user, options)
        end

        session[:server] = self
        session
      end
    rescue Net::SSH::AuthenticationFailed => error
      raise Net::SSH::AuthenticationFailed.new("#{error.message}@#{host}")
    end

    def close_channels
      session.channels.each { |id, channel| channel.close } if session
    end

    def close
      session.transport.close if session
    end

    def busy?(include_invisible=false)
      session && session.busy?(include_invisible)
    end

    def preprocess(&block)
      return true unless session
      session.preprocess(&block)
    end

    def readers
      return [] unless session
      session.listeners.keys
    end

    def writers
      return [] unless session
      session.listeners.keys.select do |io|
        io.respond_to?(:pending_write?) && io.pending_write?
      end
    end

    def postprocess(readers, writers)
      return true unless session
      listeners = session.listeners.keys
      session.postprocess(listeners & readers, listeners & writers)
    end
  end
end; end; end