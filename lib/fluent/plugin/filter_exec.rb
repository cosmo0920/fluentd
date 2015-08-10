#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'fluent/plugin/exec_util'
require 'fluent/plugin/child_process_util'

module Fluent
  class ExecFilter < Filter
    Plugin.register_filter('exec', self)

    def initialize
      super
      require 'fluent/timezone'
    end

    config_param :command, :string

    config_param :add_prefix, :string, :default => nil

    config_param :in_format, :default => :tsv do |val|
      f = ExecUtil::SUPPORTED_FORMAT[val]
      raise ConfigError, "Unsupported in_format '#{val}'" unless f
      f
    end
    config_param :in_keys, :default => [] do |val|
      val.split(',')
    end
    config_param :tag_key, :default => nil
    config_param :time_key, :default => nil
    config_param :time_format, :default => nil

    config_param :out_format, :default => :tsv do |val|
      f = ExecUtil::SUPPORTED_FORMAT[val]
      raise ConfigError, "Unsupported out_format '#{val}'" unless f
      f
    end
    config_param :out_keys, :default => [] do |val|  # for tsv format
      val.split(',')
    end

    config_param :tag, :string, :default => nil

    config_param :localtime, :bool, :default => true
    config_param :timezone, :string, :default => nil
    config_param :num_children, :integer, :default => 1

    # nil, 'none' or 0: no respawn, 'inf' or -1: infinite times, positive integer: try to respawn specified times only
    config_param :child_respawn, :string, :default => nil

    # 0: output logs for all of messages to emit
    config_param :suppress_error_log_interval, :time, :default => 0

    config_set_default :flush_interval, 1

    def configure(conf)
      super

      if @timezone
        Fluent::Timezone.validate!(@timezone)
      end

      if !@tag && !@tag_key
        raise ConfigError, "'tag' or 'tag_key' option is required on exec_filter output"
      end

      if @time_key
        if f = @time_format
          tf = TimeFormatter.new(f, @localtime, @timezone)
          @time_format_proc = tf.method(:format)
        else
          @time_format_proc = Proc.new {|time| time.to_s }
        end
      elsif @time_format
        log.warn "in_time_format effects nothing when in_time_key is not specified: #{conf}"
      end

      if @out_time_key
        if f = @out_time_format
          @time_parse_proc = Proc.new {|str| Time.strptime(str, f).to_i }
        else
          @time_parse_proc = Proc.new {|str| str.to_i }
        end
      elsif @out_time_format
        log.warn "out_time_format effects nothing when out_time_key is not specified: #{conf}"
      end

      case @in_format
      when :tsv
        if @in_keys.empty?
          raise ConfigError, "in_keys option is required on exec_filter output for tsv in_format"
        end
        @formatter = ExecUtil::TSVFormatter.new(@in_keys)
      when :json
        @formatter = ExecUtil::JSONFormatter.new
      when :msgpack
        @formatter = ExecUtil::MessagePackFormatter.new
      end

      case @out_format
      when :tsv
        if @out_keys.empty?
          raise ConfigError, "out_keys option is required on exec_filter output for tsv in_format"
        end
        @parser = ExecUtil::TSVParser.new(@out_keys, method(:on_message))
      when :json
        @parser = ExecUtil::JSONParser.new(method(:on_message))
      when :msgpack
        @parser = ExecUtil::MessagePackParser.new(method(:on_message))
      end

      @respawns = if @child_respawn.nil? or @child_respawn == 'none' or @child_respawn == '0'
                    0
                  elsif @child_respawn == 'inf' or @child_respawn == '-1'
                    -1
                  elsif @child_respawn =~ /^\d+$/
                    @child_respawn.to_i
                  else
                    raise ConfigError, "child_respawn option argument invalid: none(or 0), inf(or -1) or positive number"
                  end

      @suppress_error_log_interval ||= 0
      @next_log_time = Time.now.to_i
      @mutex = Mutex.new
    end

    def start
      super

      @children = []
      @rr = 0
      begin
        @num_children.times do
          c = ChildProcess.new(@parser, @respawns, log)
          c.start(@command)
          @children << c
        end
      rescue
        shutdown
        raise
      end
    end

    def before_shutdown
      super
      log.debug "out_exec_filter#before_shutdown called"
      @children.each {|c|
        c.finished = true
      }
      sleep 0.5  # TODO wait time before killing child process
    end

    def shutdown
      super

      @children.reject! {|c|
        c.shutdown
        true
      }
    end

    def reform(record)
      r = @rr = (@rr + 1) % @children.length
      @children[r].write_record record
    end

    def filter_stream(tag, es)
      new_es = MultiEventStream.new
      es.each { |time, record|
        out = ''
        if @time_key
          record[@time_key] = @time_format_proc.call(time)
        end
        if @tag_key
          record[@tag_key] = tag
        end
        @formatter.call(record, out)
        reformed = reform(out)
        new_es.add(time, reformed)
      }
      new_es
    end

    def on_message(record)
      filter(time, tag, record)
    end
  end
end
