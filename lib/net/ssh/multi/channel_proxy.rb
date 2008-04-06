module Net; module SSH; module Multi
  class ChannelProxy
    attr_reader :on_confirm

    def initialize(&on_confirm)
      @on_confirm = on_confirm
      @recordings = []
      @channel = nil
    end

    def delegate_to(channel)
      @channel = channel
      @recordings.each do |sym, args, block|
        @channel.__send__(sym, *args, &block)
      end
    end

    def method_missing(sym, *args, &block)
      if @channel
        @channel.__send__(sym, *args, &block)
      else
        @recordings << [sym, args, block]
      end
    end
  end
end; end; end