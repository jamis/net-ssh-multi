require 'net/ssh/multi/session'

module Net; module SSH
  module Multi
    def self.start
      session = Session.new

      if block_given?
        begin
          yield session
          session.loop
        ensure
          session.close
        end
      else
        return session
      end
    end
  end
end; end