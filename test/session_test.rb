require 'common'
require 'net/ssh/multi/session'

class SessionTest < Test::Unit::TestCase
  def setup
    @session = Net::SSH::Multi::Session.new
  end

  def test_group_should_fail_when_given_both_mapping_and_block
    assert_raises(ArgumentError) do
      @session.group(:app => mock('server')) { |s| }
    end
  end

  def test_group_with_block_should_use_groups_within_block_and_restore_on_exit
    @session.open_groups.concat([:first, :second])
    assert_equal [:first, :second], @session.open_groups
    yielded = nil
    @session.group(:third, :fourth) do |s|
      yielded = s
      assert_equal [:first, :second, :third, :fourth], @session.open_groups
    end
    assert_equal [:first, :second], @session.open_groups
    assert_equal @session, yielded
  end

  def test_group_with_mapping_should_append_new_servers_to_specified_and_open_groups
    @session.open_groups.concat([:first, :second])
    @session.groups[:second] = [1]
    @session.group %w(third fourth) => [2, 3], :fifth => 1, :sixth => [4]
    assert_equal [1, 2, 3, 4], @session.groups[:first].sort
    assert_equal [1, 2, 3, 4], @session.groups[:second].sort
    assert_equal [2, 3], @session.groups[:third]
    assert_equal [2, 3], @session.groups[:fourth]
    assert_equal [1], @session.groups[:fifth]
    assert_equal [4], @session.groups[:sixth]
  end

  def test_via_should_instantiate_and_set_default_gateway
    Net::SSH::Gateway.expects(:new).with('host', 'user', :a => :b).returns(:gateway)
    assert_equal @session, @session.via('host', 'user', :a => :b)
    assert_equal :gateway, @session.default_gateway
  end

  def test_use_should_add_new_server_to_server_list
    @session.open_groups.concat([:first, :second])
    server = @session.use('host', 'user', :a => :b)
    assert_equal [server], @session.servers
    assert_equal 'host', server.host
    assert_equal 'user', server.user
    assert_equal({:a => :b}, server.options)
    assert_nil server.gateway
  end

  def test_use_with_open_groups_should_add_new_server_to_server_list_and_groups
    @session.open_groups.concat([:first, :second])
    server = @session.use('host', 'user')
    assert_equal [server], @session.groups[:first]
    assert_equal [server], @session.groups[:second]
  end

  def test_use_with_default_gateway_should_set_gateway_on_server
    Net::SSH::Gateway.expects(:new).with('host', 'user', {}).returns(:gateway)
    @session.via('host', 'user')
    server = @session.use('host2', 'user2')
    assert_equal :gateway, server.gateway
  end

  def test_use_with_duplicate_server_will_not_add_server_twice
    s1 = @session.use('host', 'user')
    s2 = @session.use('host', 'user')
    assert_equal 1, @session.servers.length
    assert_equal s1.object_id, s2.object_id
  end

  def test_with_should_set_active_groups_and_yield_and_restore_active_groups
    yielded = nil
    @session.with(:app, :web) do |s|
      yielded = s
      assert_equal({:app => {}, :web => {}}, @session.active_groups)
    end
    assert_equal @session, yielded
    assert_equal({}, @session.active_groups)
  end

  def test_with_with_unknown_constraint_should_raise_error
    assert_raises(ArgumentError) do
      @session.with(:app => { :all => :foo }) {}
    end
  end

  def test_with_with_constraints_should_add_constraints_to_active_groups
    @session.with(:app => { :only => { :primary => true }, :except => { :backup => true } }) do |s|
      assert_equal({:app => {:only => {:primary => true}, :except => {:backup => true}}}, @session.active_groups)
    end
  end

  def test_on_should_create_ad_hoc_group_and_make_that_group_the_only_active_group
    s1 = @session.use('h1', 'u1')
    s2 = @session.use('h2', 'u2')
    yielded = nil
    @session.active_groups[:g1] = []
    @session.on(s1, s2) do |s|
      yielded = s
      assert_equal 1, @session.active_groups.size
      assert_not_equal :g1, @session.active_groups.keys.first
      assert_equal [s1, s2], @session.groups[@session.active_groups.keys.first]
    end
    assert_equal [:g1], @session.active_groups.keys
    assert_equal @session, yielded
  end

  def test_active_sessions_should_return_sessions_for_all_servers_if_active_groups_is_empty
    s1, s2, s3 = MockSession.new, MockSession.new, MockSession.new
    srv1, srv2, srv3 = @session.use('h1', 'u1'), @session.use('h2', 'u2'), @session.use('h3', 'u3')
    Net::SSH.expects(:start).with('h1', 'u1', {}).returns(s1)
    Net::SSH.expects(:start).with('h2', 'u2', {}).returns(s2)
    Net::SSH.expects(:start).with('h3', 'u3', {}).returns(s3)
    assert_equal [s1, s2, s3], @session.active_sessions.sort
  end

  def test_active_sessions_should_return_sessions_only_for_active_groups_if_active_groups_exist
    s1, s2, s3 = MockSession.new, MockSession.new, MockSession.new
    srv1, srv2, srv3 = @session.use('h1', 'u1'), @session.use('h2', 'u2'), @session.use('h3', 'u3')
    @session.group :app => [srv1, srv2], :db => [srv3]
    Net::SSH.expects(:start).with('h1', 'u1', {}).returns(s1)
    Net::SSH.expects(:start).with('h2', 'u2', {}).returns(s2)
    @session.active_groups.replace(:app => {})
    assert_equal [s1, s2], @session.active_sessions.sort
  end

  def test_active_sessions_should_not_return_duplicate_sessions
    s1, s2, s3 = MockSession.new, MockSession.new, MockSession.new
    srv1, srv2, srv3 = @session.use('h1', 'u1'), @session.use('h2', 'u2'), @session.use('h3', 'u3')
    @session.group :app => [srv1, srv2], :db => [srv2, srv3]
    Net::SSH.expects(:start).with('h1', 'u1', {}).returns(s1)
    Net::SSH.expects(:start).with('h2', 'u2', {}).returns(s2)
    Net::SSH.expects(:start).with('h3', 'u3', {}).returns(s3)
    @session.active_groups.replace(:app => {}, :db => {})
    assert_equal [s1, s2, s3], @session.active_sessions.sort
  end

  def test_active_sessions_should_correctly_apply_only_and_except_constraints
    s1, s2, s3 = MockSession.new, MockSession.new, MockSession.new
    srv1, srv2, srv3 = @session.use('h1', 'u1', :properties => {:a => 1}), @session.use('h2', 'u2', :properties => {:a => 1, :b => 2}), @session.use('h3', 'u3')
    @session.group :app => [srv1, srv2, srv3]
    Net::SSH.expects(:start).with('h1', 'u1', :properties => {:a => 1}).returns(s1)
    @session.active_groups.replace(:app => {:only => {:a => 1}, :except => {:b => 2}})
    assert_equal [s1], @session.active_sessions.sort
  end

  def test_connect_bang_should_call_active_sessions_and_return_self
    @session.expects(:active_sessions)
    assert_equal @session, @session.connect!
  end

  def test_close_should_close_server_sessions
    srv1, srv2 = @session.use('h1', 'u1'), @session.use('h2', 'u2')
    srv1.expects(:close_channels)
    srv2.expects(:close_channels)
    srv1.expects(:close)
    srv2.expects(:close)
    @session.close
  end

  def test_close_should_shutdown_default_gateway
    gateway = mock('gateway')
    gateway.expects(:shutdown!)
    Net::SSH::Gateway.expects(:new).returns(gateway)
    @session.via('host', 'user')
    @session.close
  end

  def test_busy_should_be_true_if_any_server_is_busy
    srv1, srv2, srv3 = @session.use('h1', 'u1', :properties => {:a => 1}), @session.use('h2', 'u2', :properties => {:a => 1, :b => 2}), @session.use('h3', 'u3')
    srv1.stubs(:busy?).returns(false)
    srv2.stubs(:busy?).returns(false)
    srv3.stubs(:busy?).returns(true)
    assert @session.busy?
  end

  def test_busy_should_be_false_if_all_servers_are_not_busy
    srv1, srv2, srv3 = @session.use('h1', 'u1', :properties => {:a => 1}), @session.use('h2', 'u2', :properties => {:a => 1, :b => 2}), @session.use('h3', 'u3')
    srv1.stubs(:busy?).returns(false)
    srv2.stubs(:busy?).returns(false)
    srv3.stubs(:busy?).returns(false)
    assert !@session.busy?
  end

  def test_loop_should_loop_until_process_is_false
    @session.expects(:process).with(5).times(4).returns(true,true,true,false).yields
    yielded = false
    @session.loop(5) { yielded = true }
    assert yielded
  end

  def test_preprocess_should_immediately_return_false_if_block_returns_false
    srv = @session.use('h1', 'u1')
    srv.expects(:preprocess).never
    assert_equal false, @session.preprocess { false }
  end

  def test_preprocess_should_call_preprocess_on_component_servers
    srv = @session.use('h1', 'u1')
    srv.expects(:preprocess)
    assert_equal :hello, @session.preprocess { :hello }
  end

  def test_preprocess_should_succeed_even_without_block
    srv = @session.use('h1', 'u1')
    srv.expects(:preprocess)
    assert_equal true, @session.preprocess
  end

  def test_postprocess_should_call_postprocess_on_component_servers
    srv = @session.use('h1', 'u1')
    srv.expects(:postprocess).with([:a], [:b])
    assert_equal true, @session.postprocess([:a], [:b])
  end

  def test_process_should_return_false_if_preprocess_returns_false
    assert_equal false, @session.process { false }
  end

  def test_process_should_call_select_on_combined_readers_and_writers_from_all_servers
    @session.expects(:postprocess).with([:b, :c], [:a, :c])
    srv1, srv2, srv3 = @session.use('h1', 'u1'), @session.use('h2', 'u2'), @session.use('h3', 'u3')
    srv1.expects(:readers).returns([:a])
    srv1.expects(:writers).returns([:a])
    srv2.expects(:readers).returns([])
    srv2.expects(:writers).returns([])
    srv3.expects(:readers).returns([:b, :c])    
    srv3.expects(:writers).returns([:c])
    IO.expects(:select).with([:a, :b, :c], [:a, :c], nil, 5).returns([[:b, :c], [:a, :c]])
    @session.process(5)
  end

  def test_send_global_request_should_delegate_to_active_sessions
    s1 = mock('ssh')
    s2 = mock('ssh')
    s1.expects(:send_global_request).with("a", "b", "c").yields
    s2.expects(:send_global_request).with("a", "b", "c").yields
    @session.expects(:active_sessions).returns([s1, s2])
    calls = 0
    @session.send_global_request("a", "b", "c") { calls += 1 }
    assert_equal 2, calls
  end

  def test_open_channel_should_delegate_to_active_sessions_and_set_accessors_on_each_channel_and_return_multi_channel
    srv1 = @session.use('h1', 'u1')
    srv2 = @session.use('h2', 'u2')
    s1 = { :server => srv1 }
    s2 = { :server => srv2 }
    c1 = { :stub => :value }
    c2 = {}
    c1.stubs(:connection).returns(s1)
    c2.stubs(:connection).returns(s2)
    @session.expects(:active_sessions).returns([s1, s2])
    s1.expects(:open_channel).with("session").yields(c1).returns(c1)
    s2.expects(:open_channel).with("session").yields(c2).returns(c2)
    results = []
    channel = @session.open_channel do |c|
      results << c
    end
    assert_equal [c1, c2], results
    assert_equal "h1", c1[:host]
    assert_equal "h2", c2[:host]
    assert_equal srv1, c1[:server]
    assert_equal srv2, c2[:server]
    assert_instance_of Net::SSH::Multi::Channel, channel
    assert_equal [c1, c2], channel.channels
  end

  def test_exec_should_raise_exception_if_channel_cannot_exec_command
    c = { :host => "host" }
    @session.expects(:open_channel).yields(c).returns(c)
    c.expects(:exec).with('something').yields(c, false)
    assert_raises(RuntimeError) { @session.exec("something") }
  end

  def test_exec_with_block_should_pass_data_and_extended_data_to_block
    c = { :host => "host" }
    @session.expects(:open_channel).yields(c).returns(c)
    c.expects(:exec).with('something').yields(c, true)
    c.expects(:on_data).yields(c, "stdout")
    c.expects(:on_extended_data).yields(c, 1, "stderr")
    c.expects(:on_request)
    results = {}
    @session.exec("something") do |c, stream, data|
      results[stream] = data
    end
    assert_equal({:stdout => "stdout", :stderr => "stderr"}, results)
  end

  def test_exec_without_block_should_write_data_and_extended_data_lines_to_stdout_and_stderr
    c = { :host => "host" }
    @session.expects(:open_channel).yields(c).returns(c)
    c.expects(:exec).with('something').yields(c, true)
    c.expects(:on_data).yields(c, "stdout 1\nstdout 2\n")
    c.expects(:on_extended_data).yields(c, 1, "stderr 1\nstderr 2\n")
    c.expects(:on_request)
    $stdout.expects(:puts).with("[host] stdout 1\n")
    $stdout.expects(:puts).with("[host] stdout 2")
    $stderr.expects(:puts).with("[host] stderr 1\n")
    $stderr.expects(:puts).with("[host] stderr 2")
    @session.exec("something")
  end

  def test_exec_should_capture_exit_status_of_process
    c = { :host => "host" }
    @session.expects(:open_channel).yields(c).returns(c)
    c.expects(:exec).with('something').yields(c, true)
    c.expects(:on_data)
    c.expects(:on_extended_data)
    c.expects(:on_request).with("exit-status").yields(c, Net::SSH::Buffer.from(:long, 127))
    @session.exec("something")
    assert_equal 127, c[:exit_status]
  end

  private

    class MockSession < Hash
      include Comparable

      @@next_id = 0
      attr_reader :id

      def initialize
        @id = (@@next_id += 1)
      end

      def <=>(s)
        id <=> s.id
      end
    end
end