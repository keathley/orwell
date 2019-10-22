require 'rdkafka'

config = {
  :"bootstrap.servers" => "localhost:9092",
  :"group.id" => "ping-consumer"
}
consumer = Rdkafka::Config.new(config).consumer
consumer.subscribe("pings")

consumer.each do |message|
  puts "Message received: #{message}"
  sleep 0.1
end
