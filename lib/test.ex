{:ok, connection} = AMQP.Connection.open()
{:ok, channel} = AMQP.Channel.open(connection)

AMQP.Queue.declare(channel, "telemetry_input",
  durable: true,
  arguments: [{"x-queue-type", :longstr, "classic"}]
)

device_ids = Enum.map(1..25, fn id -> "device-#{id}" end)

Enum.each(1..500_000, fn _ ->
  reading = %{device_id: Enum.random(device_ids), data: %{tmp: Enum.random(8..42)}}
  AMQP.Basic.publish(channel, "", "telemetry_input", Jason.encode!(reading))
end)

AMQP.Connection.close(connection)
