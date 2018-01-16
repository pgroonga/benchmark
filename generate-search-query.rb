filename = "0000.jsonl"


puts("BEGIN;")
File.open(filename, "r") do |file|
  file.each_line do |line|
    value = line.match(/"title":"(?<title>.*?)"/)
    puts("  SELECT * FROM fluentd WHERE record &@~ '#{value[:title]}';")
  end
end
puts("END;")
