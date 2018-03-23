class Psql
  class << self
    def open(database_name)
      psql = new(database_name)
      begin
        yield(psql)
      rescue
        $stderr.puts("#{$!}: #{$!}")
        $stderr.puts($@)
      ensure
        psql.close
      end
    end
  end

  def initialize(database_name)
    input, @output = IO.pipe
    @input, output = IO.pipe
    @pid = spawn("psql",
                 "--dbname", database_name,
                 "--no-psqlrc",
                 "--no-align",
                 "--tuples-only",
                 :in => input,
                 :out => output)
    input.close
    output.close
  end

  def execute(sql)
    @output.puts(sql)
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
