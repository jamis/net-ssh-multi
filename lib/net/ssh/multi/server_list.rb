require 'net/ssh/multi/server'
require 'net/ssh/multi/dynamic_server'

module Net; module SSH; module Multi

  class ServerList
    include Enumerable

    def initialize(list=[])
      @list = list.uniq
    end

    def add(server)
      index = @list.index(server)
      if index
        server = @list[index]
      else
        @list.push(server)
      end
      server
    end

    def concat(servers)
      servers.each { |server| add(server) }
      self
    end

    def each
      @list.each do |server|
        case server
        when Server then yield server
        when DynamicServer then server.each { |item| yield item }
        else raise ArgumentError, "server list contains non-server: #{server.class}"
        end
      end
      self
    end

    def select
      subset = @list.select { |i| yield i }
      ServerList.new(subset)
    end

    def flatten
      result = @list.inject([]) do |aggregator, server|
        case server
        when Server then aggregator.push(server)
        when DynamicServer then aggregator.concat(server)
        else raise ArgumentError, "server list contains non-server: #{server.class}"
        end
      end

      result.uniq
    end

    def to_ary
      flatten
    end
  end

end; end; end