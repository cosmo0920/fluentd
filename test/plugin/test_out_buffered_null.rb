require_relative '../helper'
require 'fluent/test'

class BufferedNullOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::BufferedNullOutput).configure(conf)
  end

  def test_create
    d = create_driver
    assert_equal Fluent::BufferedNullOutput, d.instance.class
  end

  def test_write
    d = create_driver
    time = Time.now
    d.run do
      d.emit({'test' => 'test'}, Fluent::EventTime.from_time(time))
    end

    emits = d.emits
    assert_equal [], emits
  end
end
