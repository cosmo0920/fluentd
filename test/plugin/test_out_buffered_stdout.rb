require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/out_buffered_stdout'

class BufferedStdoutOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::BufferedStdoutOutput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 'json', d.instance.output_type
  end

  def test_configure_output_type
    d = create_driver(CONFIG + "\noutput_type json")
    assert_equal 'json', d.instance.output_type

    d = create_driver(CONFIG + "\noutput_type hash")
    assert_equal 'hash', d.instance.output_type

    assert_raise(Fluent::ConfigError) do
      d = create_driver(CONFIG + "\noutput_type foo")
    end
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_emit_json(data)
    d = create_driver(CONFIG + "\noutput_type json\njson_parser #{data}")
    time = Time.now
    d.run do
      d.emit({'test' => 'test'}, Fluent::EventTime.from_time(time))
    end
    out = d.instance.log.out.logs
    assert_equal "#{time.localtime} test: {\"test\":\"test\"}\n", out

    if data == 'yajl'
      # NOTE: Float::NAN is not jsonable
      assert_raise(Yajl::EncodeError) { d.emit({'test' => Float::NAN}, time) }
    else
      d.run do
        d.emit({'test' => Float::NAN}, Fluent::EventTime.from_time(time))
      end
      out = d.instance.log.out.logs
      assert_equal "#{time.localtime} test: {\"test\":NaN}\n", out
    end
  end
end
