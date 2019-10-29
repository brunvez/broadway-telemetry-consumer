defmodule MyBroadway do
  use Broadway

  alias Broadway.Message

  @subscriptions [
    %{
      id: 1,
      endpoint: "http://localhost:3333/telemetry/1",
      devices: [
        %{id: "device-17"},
        %{id: "device-15"},
        %{id: "device-9"},
        %{id: "device-14"},
        %{id: "device-20"},
        %{id: "device-16"}
      ]
    },
    %{
      id: 2,
      endpoint: "http://localhost:3333/telemetry/2",
      devices: [
        %{id: "device-15"},
        %{id: "device-11"},
        %{id: "device-4"},
        %{id: "device-2"},
        %{id: "device-20"}
      ]
    },
    %{
      id: 3,
      endpoint: "http://localhost:3333/telemetry/3",
      devices: [
        %{id: "device-25"},
        %{id: "device-22"},
        %{id: "device-13"},
        %{id: "device-21"},
        %{id: "device-9"},
        %{id: "device-15"}
      ]
    },
    %{
      id: 4,
      endpoint: "http://localhost:3333/telemetry/4",
      devices: [
        %{id: "device-14"},
        %{id: "device-8"},
        %{id: "device-20"},
        %{id: "device-4"},
        %{id: "device-18"},
        %{id: "device-10"}
      ]
    },
    %{
      id: 5,
      endpoint: "http://localhost:3333/telemetry/5",
      devices: [
        %{id: "device-8"},
        %{id: "device-23"},
        %{id: "device-25"},
        %{id: "device-5"},
        %{id: "device-3"}
      ]
    },
    %{
      id: 6,
      endpoint: "http://localhost:3333/telemetry/6",
      devices: [
        %{id: "device-8"},
        %{id: "device-13"},
        %{id: "device-1"},
        %{id: "device-24"},
        %{id: "device-22"}
      ]
    },
    %{
      id: 7,
      endpoint: "http://localhost:3333/telemetry/7",
      devices: [
        %{id: "device-14"},
        %{id: "device-4"},
        %{id: "device-3"},
        %{id: "device-15"},
        %{id: "device-25"},
        %{id: "device-19"},
        %{id: "device-16"}
      ]
    },
    %{
      id: 8,
      endpoint: "http://localhost:3333/telemetry/8",
      devices: [
        %{id: "device-8"},
        %{id: "device-12"},
        %{id: "device-22"},
        %{id: "device-20"},
        %{id: "device-6"},
        %{id: "device-4"}
      ]
    },
    %{
      id: 9,
      endpoint: "http://localhost:3333/telemetry/10",
      devices: [
        %{id: "device-5"},
        %{id: "device-14"},
        %{id: "device-16"},
        %{id: "device-24"},
        %{id: "device-2"},
        %{id: "device-13"},
        %{id: "device-3"}
      ]
    },
    %{
      id: 10,
      endpoint: "http://localhost:3333/telemetry/11",
      devices: [%{id: "device-11"}, %{id: "device-16"}, %{id: "device-1"}, %{id: "device-10"}]
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
    HTTPoison.post(endpoint, Jason.encode!(%{readings: readings}), [{"Content-Type", "application/json"}])
  end
end
