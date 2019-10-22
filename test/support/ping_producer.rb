require 'kafka'

trap "SIGINT" do
  puts "Exiting..."
  exit 130
end

kafka = Kafka.new(["localhost:9092"], client_id: 'ping-test')

thread = Thread.new do
  puts "Producing =>"
  loop do
    printf '.'
    partition = rand(3)
    kafka.deliver_message("ping", topic: "pings", partition: partition)
    sleep 0.3
  end
end

thread.join

