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
    kafka.deliver_message("ping", topic: "pings")
    sleep 0.5
  end
end

thread.join

