defmodule Shortener.Aggregates do
  use GenServer

  alias __MODULE__
  alias Shortener.GCounter

  require Logger

  def count_for(table \\ __MODULE__, hash) do
    case :ets.lookup(__MODULE__, hash) do
      [] -> 0
      [{_hash, count}] -> count
    end
  end

  def increment(server \\ __MODULE__, hash) do
    GenServer.cast(server, {:increment, hash})
  end

  def merge(server \\ __MODULE__, hash, counter) do
    GenServer.abcast(server, {:merge, hash, counter})
  end

  def flush(server \\ __MODULE__) do
    GenServer.call(server, :flush)
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(_args \\ []) do
    :net_kernel.monitor_nodes(true)

    :ets.new(__MODULE__, [:named_table, read_concurrency: true])

    {:ok, %{table: __MODULE__, counters: %{}}}
  end

  def handle_cast({:increment, short_code}, %{counters: counters} = data) do
    counters = Map.put_new(counters, short_code, GCounter.new())
    counters = Map.update!(counters, short_code, &GCounter.increment/1)

    counter = Map.get(counters, short_code)

    :abcast = merge(short_code, counter)

    :ets.insert(__MODULE__, {short_code, GCounter.to_i(counter)})

    {:noreply, %{data | counters: counters}}
  end

  def handle_cast({:merge, short_code, counter}, data) do
    counters = Map.put_new(data.counters, short_code, GCounter.new())

    counters =
      Map.update!(counters, short_code, fn old ->
        GCounter.merge(old, counter)
      end)

    counter = Map.get(counters, short_code)

    :ets.insert(__MODULE__, {short_code, GCounter.to_i(counter)})

    {:noreply, %{data | counters: counters}}
  end

  def handle_call(:flush, _from, data) do
    :ets.delete_all_objects(data.table)

    {:reply, :ok, %{data | counters: %{}}}
  end

  def handle_info({:nodeup, node}, data) do
    Enum.each(data.counters, fn {short_code, counter} ->
      :abcast = merge(short_code, counter)
    end)

    {:noreply, data}
  end

  def handle_info({:nodedown, _node}, data) do
    {:noreply, data}
  end

  def handle_info(msg, data) do
    Logger.info("Unhandled message: #{inspect(msg)}")

    {:noreply, data}
  end
end
