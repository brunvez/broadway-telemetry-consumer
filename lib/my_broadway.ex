defmodule MyBroadway do
  use Broadway

  alias Broadway.Message

  @subscriptions [
    %Subscription{
      id: 1,
      delay: 10,
      endpoint: "http://localhost:3333/telemetry/1",
      devices: [
        %Device{id: "device-17", name: "Device 17"},
        %Device{id: "device-15", name: "Device 15"},
        %Device{id: "device-9", name: "Device 9"},
        %Device{id: "device-14", name: "Device 14"},
        %Device{id: "device-20", name: "Device 20"},
        %Device{id: "device-16", name: "Device 16"}
      ]
    },
    %Subscription{
      id: 2,
      delay: 20,
      endpoint: "http://localhost:3333/telemetry/2",
      devices: [
        %Device{id: "device-15", name: "Device 15"},
        %Device{id: "device-11", name: "Device 11"},
        %Device{id: "device-4", name: "Device 4"},
        %Device{id: "device-2", name: "Device 2"},
        %Device{id: "device-20", name: "Device 20"}
      ]
    },
    %Subscription{
      id: 3,
      delay: 30,
      endpoint: "http://localhost:3333/telemetry/3",
      devices: [
        %Device{id: "device-25", name: "Device 25"},
        %Device{id: "device-22", name: "Device 22"},
        %Device{id: "device-13", name: "Device 13"},
        %Device{id: "device-21", name: "Device 21"},
        %Device{id: "device-9", name: "Device 9"},
        %Device{id: "device-15", name: "Device 15"}
      ]
    },
    %Subscription{
      id: 4,
      delay: 40,
      endpoint: "http://localhost:3333/telemetry/4",
      devices: [
        %Device{id: "device-14", name: "Device 14"},
        %Device{id: "device-8", name: "Device 8"},
        %Device{id: "device-20", name: "Device 20"},
        %Device{id: "device-4", name: "Device 4"},
        %Device{id: "device-18", name: "Device 18"},
        %Device{id: "device-10", name: "Device 10"}
      ]
    },
    %Subscription{
      id: 5,
      delay: 50,
      endpoint: "http://localhost:3333/telemetry/5",
      devices: [
        %Device{id: "device-8", name: "Device 8"},
        %Device{id: "device-23", name: "Device 23"},
        %Device{id: "device-25", name: "Device 25"},
        %Device{id: "device-5", name: "Device 5"},
        %Device{id: "device-3", name: "Device 3"}
      ]
    },
    %Subscription{
      id: 6,
      delay: 60,
      endpoint: "http://localhost:3333/telemetry/6",
      devices: [
        %Device{id: "device-8", name: "Device 8"},
        %Device{id: "device-13", name: "Device 13"},
        %Device{id: "device-1", name: "Device 1"},
        %Device{id: "device-24", name: "Device 24"},
        %Device{id: "device-22", name: "Device 22"}
      ]
    },
    %Subscription{
      id: 7,
      delay: 70,
      endpoint: "http://localhost:3333/telemetry/7",
      devices: [
        %Device{id: "device-14", name: "Device 14"},
        %Device{id: "device-4", name: "Device 4"},
        %Device{id: "device-3", name: "Device 3"},
        %Device{id: "device-15", name: "Device 15"},
        %Device{id: "device-25", name: "Device 25"},
        %Device{id: "device-19", name: "Device 19"},
        %Device{id: "device-16", name: "Device 16"}
      ]
    },
    %Subscription{
      id: 8,
      delay: 80,
      endpoint: "http://localhost:3333/telemetry/8",
      devices: [
        %Device{id: "device-8", name: "Device 8"},
        %Device{id: "device-12", name: "Device 12"},
        %Device{id: "device-22", name: "Device 22"},
        %Device{id: "device-20", name: "Device 20"},
        %Device{id: "device-6", name: "Device 6"},
        %Device{id: "device-4", name: "Device 4"}
      ]
    },
    %Subscription{
      id: 9,
      delay: 90,
      endpoint: "http://localhost:3333/telemetry/10",
      devices: [
        %Device{id: "device-5", name: "Device 5"},
        %Device{id: "device-14", name: "Device 14"},
        %Device{id: "device-16", name: "Device 16"},
        %Device{id: "device-24", name: "Device 24"},
        %Device{id: "device-2", name: "Device 2"},
        %Device{id: "device-13", name: "Device 13"},
        %Device{id: "device-3", name: "Device 3"}
      ]
    },
    %Subscription{
      id: 10,
      delay: 100,
      endpoint: "http://localhost:3333/telemetry/11",
      devices: [
        %Device{id: "device-11", name: "Device 11"},
        %Device{id: "device-16", name: "Device 16"},
        %Device{id: "device-1", name: "Device 1"},
        %Device{id: "device-10", name: "Device 10"}
      ]
    }
  ]

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: MyBroadway,
      producers: [
        default: [
          module:
            {BroadwayRabbitMQ.Producer,
             queue: "telemetry_input",
             qos: [
               prefetch_count: 150
             ]},
          stages: 4
        ]
      ],
      processors: [
        default: [
          stages: 11
        ]
      ],
      batchers: [
        default: [
          batch_size: 100,
          batch_timeout: 1500,
          stages: 5
        ]
      ]
    )
  end

  def handle_message(_, message, _) do
    message
    |> Message.update_data(fn data -> Jason.decode!(data) end)
  end

  def handle_batch(_, messages, _, _) do
    readings = messages |> group_readings_by_subscriptions()

    readings
    |> Enum.map(fn {id, data} ->
      [data, Enum.find(@subscriptions, &(&1.id == id))]
    end)
    |> Enum.each(fn [data, subscription] -> send_readings_to_subscription(data, subscription) end)

    messages
  end

  defp group_readings_by_subscriptions(messages) do
    messages
    |> Enum.map(fn message -> message.data end)
    |> Enum.reduce(%{}, fn message_data, readings ->
      subscriptions_with_device(message_data["device_id"])
      |> add_data_to_subscription_readings(readings, message_data)
    end)
  end

  defp subscriptions_with_device(device_id) do
    @subscriptions
    |> Enum.filter(fn subscription ->
      Enum.any?(subscription.devices, &(&1.id == device_id))
    end)
  end

  defp add_data_to_subscription_readings(subscriptions, readings, message_data) do
    Enum.reduce(subscriptions, readings, fn subscription, acc ->
      Map.update(
        readings,
        subscription.id,
        [message_data["data"]],
        &[message_data["data"] | &1]
      )
    end)
  end

  defp send_readings_to_subscription(readings, %{endpoint: endpoint} = _subscription) do
    HTTPoison.post(endpoint, Jason.encode!(%{readings: readings}), [
      {"Content-Type", "application/json"}
    ])
  end
end
