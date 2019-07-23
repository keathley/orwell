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

consumer.each_message do |message|
  puts message.offset, message.key, message.value
  producer.produce("pong", topic: "pongs")
  producer.deliver_messages
  sleep 0.5
end

