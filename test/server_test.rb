require 'common'
require 'net/ssh/multi/server'

class ServerTest < Test::Unit::TestCase
  def setup
    @master = mock('multi-session')
  end

  def test_accessor_without_properties_should_access_empty_hash
    assert_nil server('host', 'user')[:foo]
  end

  def test_accessor_with_properties_should_access_properties
    assert_equal "hello", server('host', 'user', :properties => { :foo => "hello" })[:foo]
  end

  def test_port_should_return_22_by_default
    assert_equal 22, server('host', 'user').port
  end

  def test_port_should_return_given_port_when_present
    assert_equal 1234, server('host', 'user', :port => 1234).port
  end

  def test_equivalence_when_host_and_user_and_port_match
    s1 = server('host', 'user', :port => 1234)
    s2 = server('host', 'user', :port => 1234)
    assert s1.eql?(s2)
    assert_equal s1.hash, s2.hash
    assert s1 == s2
  end

  def test_equivalence_when_host_mismatch
    s1 = server('host1', 'user', :port => 1234)
    s2 = server('host2', 'user', :port => 1234)
    assert !s1.eql?(s2)
    assert_not_equal s1.hash, s2.hash
    assert s1 != s2
  end

  def test_equivalence_when_port_mismatch
    s1 = server('host', 'user', :port => 1234)
    s2 = server('host', 'user', :port => 1235)
    assert !s1.eql?(s2)
    assert_not_equal s1.hash, s2.hash
    assert s1 != s2
  end

  def test_equivalence_when_user_mismatch
    s1 = server('host', 'user1', :port => 1234)
    s2 = server('host', 'user2', :port => 1234)
    assert !s1.eql?(s2)
    assert_not_equal s1.hash, s2.hash
    assert s1 != s2
  end

  def test_to_s_should_include_user_and_host
    assert_equal "user@host", server('host', 'user').to_s
  end

  def test_to_s_should_include_user_and_host_and_port_when_port_is_given
    assert_equal "user@host:1234", server('host', 'user', :port => 1234).to_s
  end

  def test_gateway_should_be_nil_by_default
    assert_nil server('host', 'user').gateway
  end

  def test_gateway_should_be_set_with_the_via_value
    gateway = mock('gateway')
    assert_equal gateway, server('host', 'user', :via => gateway).gateway
  end

  def test_session_with_default_argument_should_not_instantiate_session
    assert_nil server('host', 'user').session
  end

  def test_session_with_true_argument_should_instantiate_and_cache_session
    srv = server('host', 'user', :port => 1234)
    session = expect_connection_to(srv)
    assert_equal session, srv.session(true)
    assert_equal session, srv.session(true)
    assert_equal session, srv.session
  end

  def test_session_that_cannot_authenticate_adds_host_to_exception_message
    srv = server('host', 'user')
    Net::SSH.expects(:start).with('host', 'user', {}).raises(Net::SSH::AuthenticationFailed.new('user'))

    begin
      srv.new_session
      flunk
    rescue Net::SSH::AuthenticationFailed => e
      assert_equal "user@host", e.message
    end
  end

  def test_close_channels_when_session_is_not_open_should_not_do_anything
    assert_nothing_raised { server('host', 'user').close_channels }
  end

  def test_close_channels_when_session_is_open_should_iterate_over_open_channels_and_close_them
    srv = server('host', 'user')
    session = expect_connection_to(srv)
    c1 = mock('channel', :close => nil)
    c2 = mock('channel', :close => nil)
    c3 = mock('channel', :close => nil)
    session.expects(:channels).returns(1 => c1, 2 => c2, 3 => c3)
    assert_equal session, srv.session(true)
    srv.close_channels
  end

  def test_close_when_session_is_not_open_should_not_do_anything
    assert_nothing_raised { server('host', 'user').close }
  end

  def test_close_when_session_is_open_should_close_session
    srv = server('host', 'user')
    session = expect_connection_to(srv)
    session.expects(:close)
    @master.expects(:server_closed).with(srv)
    assert_equal session, srv.session(true)
    srv.close
  end

  def test_busy_should_be_false_when_session_is_not_open
    assert !server('host', 'user').busy?
  end

  def test_busy_should_be_false_when_session_is_not_busy
    srv = server('host', 'user')
    session = expect_connection_to(srv)
    session.expects(:busy?).returns(false)
    srv.session(true)
    assert !srv.busy?
  end

  def test_busy_should_be_true_when_session_is_busy
    srv = server('host', 'user')
    session = expect_connection_to(srv)
    session.expects(:busy?).returns(true)
    srv.session(true)
    assert srv.busy?
  end

  def test_preprocess_should_be_nil_when_session_is_not_open
    assert_nil server('host', 'user').preprocess
  end

  def test_preprocess_should_return_result_of_session_preprocess
    srv = server('host', 'user')
    session = expect_connection_to(srv)
    session.expects(:preprocess).returns(:result)
    srv.session(true)
    assert_equal :result, srv.preprocess
  end

  def test_readers_should_return_empty_array_when_session_is_not_open
    assert_equal [], server('host', 'user').readers
  end

  def test_readers_should_return_all_listeners_when_session_is_open
    srv = server('host', 'user')
    session = expect_connection_to(srv)
    session.expects(:listeners).returns(1 => 2, 3 => 4, 5 => 6, 7 => 8)
    srv.session(true)
    assert_equal [1, 3, 5, 7], srv.readers.sort
  end

  def test_writers_should_return_empty_array_when_session_is_not_open
    assert_equal [], server('host', 'user').writers
  end

  def test_writers_should_return_all_listeners_that_are_pending_writes_when_session_is_open
    srv = server('host', 'user')
    session = expect_connection_to(srv)
    listeners = { writer(:ready) => 1, writer(:reader) => 2,
      writer(:reader) => 3, writer(:idle) => 4, writer(:ready) => 5 }
    session.expects(:listeners).returns(listeners)
    srv.session(true)
    assert_equal 2, srv.writers.length
  end

  def test_postprocess_should_return_true_when_session_is_not_open
    assert_equal true, server('host', 'user').postprocess([], [])
  end

  def test_postprocess_should_call_session_postprocess_with_ios_belonging_to_session
    srv = server('host', 'user')
    session = expect_connection_to(srv)
    session.expects(:listeners).returns(1 => 2, 3 => 4, 5 => 6, 7 => 8)
    session.expects(:postprocess).with([1,3], [7]).returns(:result)
    srv.session(true)
    assert_equal :result, srv.postprocess([1,11,3], [18,14,7,12])
  end

  private

    def server(host, user, options={})
      Net::SSH::Multi::Server.new(@master, host, user, options)
    end

    def expect_connection_to(server)
      session = {}
      @master.expects(:next_session).with(server).returns(session)
      return session
    end

    def writer(mode)
      case mode
      when :ready then
        stub('io', :pending_write? => true)
      when :idle then
        stub('io', :pending_write? => false)
      else
        stub('io')
      end
    end
end