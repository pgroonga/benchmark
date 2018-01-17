values = []
Dir.glob('*') do |item|
  filename = item
  File.open(filename, "r") do |file|
    file.each_line do |line|
      tag = line.match(/"tags":\["(?<tag>.*?)"/)
      if tag != nil
        values << tag[:tag]
      end
    end
  end
end
values = values.uniq
puts("BEGIN;")
  values.each{|value|
    puts("  SELECT * FROM fluentd WHERE record &` 'paths == \"title\" && string @ \"#{value}\"';") 
  }
puts("END;")
