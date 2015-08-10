require_relative '../helper'
require 'fluent/plugin/filter_exec'
require 'timecop'
require 'flexmock'

class ExecFilterTest < Test::Unit::TestCase
  include Fluent

  def setup
    Fluent::Test.setup
    @time = Fluent::Engine.now.to_i
  end

  CONFIG = %[
    command cat
    in_keys time,tag,k1
    out_keys time,tag,k2
    tag_key tag
    time_key time
    time_format %Y-%m-%d %H:%M:%S
    localtime
    num_children 3
  ]

  def create_driver(conf = CONFIG, tag = 'test', use_v1=false)
    Fluent::Test::FilterTestDriver.new(Fluent::ExecFilter, tag).configure(conf, use_v1)
  end

  def sed_unbuffered_support?
    @sed_unbuffered_support ||= lambda {
      system("echo xxx | sed --unbuffered -l -e 's/x/y/g' >/dev/null 2>&1")
      $?.success?
    }.call
  end

  def sed_unbuffered_option
    sed_unbuffered_support? ? '--unbuffered' : ''
  end

  def test_configure
    d = create_driver

    assert_equal ["time","tag","k1"], d.instance.in_keys
    assert_equal ["time","tag","k2"], d.instance.out_keys
    assert_equal "tag", d.instance.tag_key
    assert_equal "time", d.instance.time_key
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.time_format
    assert_equal true, d.instance.localtime
    assert_equal 3, d.instance.num_children

    d = create_driver %[
      command sed -l -e s/foo/bar/
      in_keys time,k1
      out_keys time,k2
      tag xxx
      time_key time
      num_children 3
    ]
    assert_equal "sed -l -e s/foo/bar/", d.instance.command
  end

  def test_filter_1
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15").to_i
    d.run do
      d.filter({"k1"=>1}, time)
    end

    filtered = d.filtered_as_array
    log_time = Time.at(time)
    assert_equal ["#{log_time.strftime("%Y-%m-%d %H:%M:%S")}\ttest\t1\n"], filtered.map {|m| m[2]}
  end

  def test_filter_2
    conf =  %[
      command cat
      in_keys time,k1
      out_keys time,k2
      tag xxx
      time_key time
      num_children 3
    ]
    d = create_driver(conf, "xxx")

    time = Time.parse("2011-01-02 13:14:15").to_i
    d.run do
      d.filter({"k1"=>1}, time)
    end

    filtered = d.filtered_as_array
    assert_equal ["#{time}\t1\n"], filtered.map {|m| m[2]}
  end

  def test_filter_3
    d = create_driver(%[
      command sed #{sed_unbuffered_option} -l -e s/foo/bar/
      in_keys time,val1
      out_keys time,val2
      tag xxx
      time_key time
      num_children 3
      out_format json
    ], 'xxx')

    time = Time.parse("2011-01-02 13:14:15").to_i

    d.run do
      d.filter({"val1"=>"sed-ed value foo"}, time)
    end

    filtered = d.filtered_as_array
    assert_equal ["#{time}\tsed-ed value bar\n"], filtered.map {|m| m[2]}
  end

  def test_filter_4
    d = create_driver(%[
      command sed #{sed_unbuffered_option} -l -e s/foo/bar/
      in_keys tag,time,val1
      remove_prefix input
      out_keys tag,time,val2
      add_prefix output
      tag_key tag
      time_key time
      num_children 3
    ], 'output.test')

    time = Time.parse("2011-01-02 13:14:15").to_i

    d.run do
      d.filter({"val1"=>"sed-ed value foo"}, time)
    end

    filtered = d.filtered_as_array
    assert_equal ["output.test\t#{time}\tsed-ed value bar\n"],
                 filtered.map {|m| m[2]}
  end

  def test_json_1
    conf =  %[
      command cat
      in_keys time,tag,k1
      out_keys time,tag,k1
      tag_key tag
      tag xxx
      time_key time
      num_children 3
      in_format json
      out_format json
    ]
    d = create_driver(conf, "json.format")

    time = Time.parse("2011-01-02 13:14:15").to_i
    d.run do
      d.filter({"k1"=>1}, time)
    end

    filtered = d.filtered_as_array
    assert_equal ["{\"k1\":1,\"time\":\"1293941655\",\"tag\":\"json.format\"}\n"],
                 filtered.map {|m| m[2]}
  end
end
