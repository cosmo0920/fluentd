require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/out_buffered_null'

class BufferedNullOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::BufferedNullOutput).configure(conf)
  end

  def test_write
    d = create_driver
    time = Time.now
    d.run do
      (1..10).each do
        d.emit({'test' => 'test'}, Fluent::EventTime.from_time(time))
      end
    end

    emits = d.emits
    assert_equal [], emits
  end
end
