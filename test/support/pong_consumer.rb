require 'kafka'

kafka = Kafka.new(["localhost:9092"], client_id: 'ping-test')

consumer = kafka.consumer(group_id: "ping-consumer")

consumer.subscribe("pongs")

trap "SIGINT" do
  puts "Exiting..."
  consumer.stop
  exit 130
end

puts "Starting consumer..."

consumer.each_message do |message|
  puts message.offset, message.key, message.value
  # sleep 0.5
end
