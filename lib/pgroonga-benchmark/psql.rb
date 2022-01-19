require "json"

module PGroongaBenchmark
  class Psql
    class << self
      def open(**options)
        psql = new(**options)
        begin
          yield(psql)
        rescue => error
          $stderr.puts("#{error.class}: #{error}")
          $stderr.puts($@)
        ensure
          psql.close
        end
      end

      def run(sql, type: nil, **options)
        open(**options) do |psql|
          response = psql.execute(sql)
          response << psql.finish
          psql.close

          case type
          when :integer
            Integer(response, 10)
          when :string
            response.chomp
          when :json
            JSON.parse(response)
          else
            response
          end
        end
      end

      def run_groonga(command, **options)
        response = run("SELECT pgroonga_command('#{command}')",
                       type: :json,
                       **options)
        header, body = response
        unless header[0].zero?
          message = "Failed to execute Groonga command: "
          message << "#{header.inspect}: <#{command}>"
          raise message
        end
        body
      end
    end

    def initialize(host: nil,
                   port: nil,
                   user: nil,
                   database: nil)
      input, @output = IO.pipe
      @input, output = IO.pipe
      command_line = ["psql"]
      command_line << "--host=#{host}" if host
      command_line << "--port=#{port}" if port
      command_line << "--username=#{user}" if user
      command_line << "--dbname=#{database}" if database
      command_line << "--no-psqlrc"
      command_line << "--no-align"
      command_line << "--tuples-only"
      @pid = spawn(*command_line,
                   :in => input,
                   :out => output)
      input.close
      output.close
    end

    def execute(sql)
      if sql.is_a?(IO)
        IO.copy_stream(sql, @output)
      else
        @output.puts(sql)
      end
      @output.flush
      read
    end

    def finish
      @output.close
      @input.read
    end

    def read(timeout=0)
      response = ""
      loop do
        break if IO.select([@input], nil, nil, timeout).nil?
        response << @input.gets
        timeout *= 0.1
      end
      response
    end

    def close
      return if @pid.nil?
      @input.close
      @output.close unless @output.closed?
      pid, status = Process.waitpid2(@pid)
      @pid = nil
      unless status.success?
        raise "Failed to run psql: #{status.to_i}"
      end
    end
  end
end
