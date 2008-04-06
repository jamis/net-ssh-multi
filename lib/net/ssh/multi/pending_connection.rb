require 'net/ssh/multi/channel_proxy'

module Net; module SSH; module Multi
  class PendingConnection
    class ChannelOpenRecording #:nodoc:
      attr_reader :type, :extras, :channel

      def initialize(type, extras, channel)
        @type, @extras, @channel = type, extras, channel
      end

      def replay_on(session)
        real_channel = session.open_channel(type, *extras, &channel.on_confirm)
        channel.delegate_to(real_channel)
      end
    end

    class SendGlobalRequestRecording #:nodoc:
      attr_reader :type, :extra, :callback

      def initialize(type, extra, callback)
        @type, @extra, @callback = type, extra, callback
      end

      def replay_on(session)
        session.send_global_request(type, *extra, &callback)
      end
    end

    attr_reader :server

    def initialize(server)
      @server = server
      @recordings = []
    end

    def replace_with(session)
      @recordings.each { |recording| recording.replay_on(session) }
      @server.replace_session(session)
    end

    def open_channel(type="session", *extras, &on_confirm)
      channel = ChannelProxy.new(&on_confirm)
      @recordings << ChannelOpenRecording.new(type, extras, channel)
      return channel
    end

    def send_global_request(type, *extra, &callback)
      @recordings << SendGlobalRequestRecording.new(type, extra, callback)
      self
    end

    def busy?(include_invisible=false)
      true
    end

    def close
      self
    end

    def channels
      []
    end

    def preprocess
      true
    end

    def postprocess(readers, writers)
      true
    end

    def listeners
      {}
    end
  end
end; end; end