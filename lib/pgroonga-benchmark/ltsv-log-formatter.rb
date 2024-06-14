module PGroongaBenchmark
  class LTSVLogFormatter
    def call(severity, time, program_name, message)
      prefix = "timestamp:%{timestamp}\tseverity:%{severity}\tpid:%{pid}" % {
        severity: severity,
        timestamp: time.strftime("%Y-%m-%dT%H:%M:%S.%N"),
        pid: Process.pid,
      }
      formatted = +""
      backtrace = nil
      case message
      when String
      when Exception
        backtrace = message.backtrace
        message = "#{message.class}: #{message}"
      else
        message = message.inspect
      end
      message.each_line(chomp: true) do |line, i|
        formatted << "#{prefix}\tmessage:#{escape_value(line)}\n"
      end
      if backtrace
        backtrace.each do |trace|
          formatted << "#{prefix}\tmessage:#{escape_value(trace)}\n"
        end
      end
      formatted
    end

    private
    def escape_value(value)
      value.gsub(/[\t\r\n]/, " ")
    end
  end
end
