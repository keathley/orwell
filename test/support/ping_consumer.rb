require 'kafka'

kafka = Kafka.new(["localhost:9092"], client_id: 'ping-test')

consumer = kafka.consumer(group_id: "ping-consumer")
producer = kafka.producer

consumer.subscribe("pings")

trap "SIGINT" do
  puts "Exiting..."
  producer.shutdown
  consumer.stop
  exit 130
end

puts "Starting consumer"

consumer.each_message do |message|
  puts message.topic, message.partition, message.offset, message.value
  partition = rand(3)
  producer.produce("pong", topic: "pongs", partition: partition)
  producer.deliver_messages
  sleep 0.2
end

