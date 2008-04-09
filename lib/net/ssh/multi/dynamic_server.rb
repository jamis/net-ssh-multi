require 'net/ssh/multi/server'

module Net; module SSH; module Multi

  class DynamicServer
    attr_reader :master
    attr_reader :callback
    attr_reader :options

    def initialize(master, options, callback)
      @master, @options, @callback = master, options, callback
      @servers = nil
    end

    def [](key)
      (options[:properties] || {})[key]
    end

    def each
      (@servers || []).each { |server| yield server }
    end

    def evaluate!
      @servers ||= Array(callback[options]).map do |server|
          case server
          when String then Net::SSH::Multi::Server.new(master, server, options)
          else server
          end
        end
    end

    def to_ary
      evaluate!
    end
  end

end; end; end