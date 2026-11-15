using Shared.Events;

namespace Broker;

/// <summary>
/// Доставка одного события одному подписчику: изоляция отказа, таймаут, повторные попытки
/// и временное хранение для тех, кто умеет переподключаться.
/// </summary>
public class DeliveryManager
{
    /// Потолки попыток: третья — последняя.
    public const int MaxAttempts = 3;

    private readonly object _sync = new();
    private readonly Dictionary<string, SemaphoreSlim> _gates = new(StringComparer.OrdinalIgnoreCase);
    private readonly string _brokerId;
    private readonly EventJournal _journal;
    private readonly int _retryDelayMs;
    private readonly int _deliveryTimeoutMs;

    public DeliveryManager(string brokerId, EventJournal journal, int retryDelayMs = 1500, int deliveryTimeoutMs = 10000)
    {
        _brokerId = brokerId;
        _journal = journal;
        _retryDelayMs = retryDelayMs;
        _deliveryTimeoutMs = deliveryTimeoutMs;
    }

    /// Одна попытка доставки. Не бросает исключений: отказ подписчика — это результат, а не авария Broker.
    public async Task<DeliveryOutcome> DeliverAsync(SubscriberChannel channel, DomainEvent evt, int attempt)
    {
        _journal.Write(_brokerId, $"{evt.EventType} #{evt.EntityId} -> {channel.Name} (attempt {attempt})");

        if (!channel.Online || channel.Send == null)
        {
            _journal.Write(channel.Name, $"UNAVAILABLE {evt.EventType}: подписчик не на связи");
            return new DeliveryOutcome { Status = "UNAVAILABLE", Message = "подписчик не на связи" };
        }

        using var timeout = new CancellationTokenSource(_deliveryTimeoutMs);

        try
        {
            DeliveryResultMessage ack = await channel.Send(
                new DeliveryMessage { Event = evt, Attempt = attempt },
                timeout.Token);

            ack.Status = string.IsNullOrWhiteSpace(ack.Status) ? (ack.Success ? "OK" : "ERROR") : ack.Status.ToUpperInvariant();

            if (ack.Success)
            {
                _journal.Write(channel.Name, $"SUCCESS {evt.EventType} за {ack.ProcessedMs} мс");
            }
            else if (ack.Status == "UNAVAILABLE")
            {
                _journal.Write(channel.Name, $"UNAVAILABLE {evt.EventType}: {ack.Message}");
            }
            else
            {
                _journal.Write(channel.Name, $"{ack.Status} {evt.EventType}: {ack.Message}");
            }

            return new DeliveryOutcome
            {
                Success = ack.Success,
                Status = ack.Status,
                Message = ack.Message,
                ProcessedMs = ack.ProcessedMs
            };
        }
        catch (OperationCanceledException)
        {
            _journal.Write(channel.Name, $"TIMEOUT {evt.EventType}: подписчик не ответил за {_deliveryTimeoutMs} мс");
            return new DeliveryOutcome { Status = "TIMEOUT", Message = $"нет ответа за {_deliveryTimeoutMs} мс" };
        }
        catch (Exception ex) when (ex is PubSubTransportException or IOException or System.Net.Sockets.SocketException)
        {
            _journal.Write(channel.Name, $"CONNECTION_LOST {evt.EventType}: {ex.Message}");
            return new DeliveryOutcome { Status = "CONNECTION_LOST", Message = ex.Message };
        }
        catch (Exception ex)
        {
            _journal.Write(channel.Name, $"ERROR {evt.EventType}: {ex.Message}");
            return new DeliveryOutcome { Status = "ERROR", Message = ex.Message };
        }
    }

    /// Полная обработка события подписчиком: попытки подряд с паузой между ними.
    /// Семафор на подписчика держит доставки одной очереди — события не перепутаются местами.
    public async Task<DeliveryOutcome> DeliverWithRetriesAsync(SubscriberChannel channel, DomainEvent evt)
    {
        SemaphoreSlim gate = GateFor(channel.Name);
        await gate.WaitAsync();

        DeliveryOutcome last = new() { Status = "UNAVAILABLE" };

        try
        {
            for (int attempt = 1; attempt <= MaxAttempts; attempt++)
            {
                last = await DeliverAsync(channel, evt, attempt);

                if (last.Success)
                {
                    return last;
                }

                // После UNAVAILABLE, CONNECTION_LOST и TIMEOUT повторять бессмысленно: решение о очереди принимает Broker.
                if (last.Status is "UNAVAILABLE" or "CONNECTION_LOST" or "TIMEOUT")
                {
                    return last;
                }

                if (attempt < MaxAttempts && _retryDelayMs > 0)
                {
                    await Task.Delay(_retryDelayMs);
                }
            }
        }
        finally
        {
            gate.Release();
        }

        return last;
    }

    /// Повторная доставка накопленных событий после восстановления подписчика.
    public async Task<int> FlushQueueAsync(SubscriberChannel channel, EventQueue queue)
    {
        SemaphoreSlim gate = GateFor(channel.Name);
        await gate.WaitAsync();

        int delivered = 0;

        try
        {
            while (true)
            {
                DomainEvent? evt = queue.Peek(channel.Name);

                if (evt == null)
                {
                    return delivered;
                }

                if (!queue.TryRedeliver(channel.Name, evt))
                {
                    queue.Complete(channel.Name, evt.EventId);
                    _journal.Write(_brokerId,
                        $"DEAD LETTER {evt.EventType} #{evt.EntityId} -> {channel.Name}: {EventQueue.MaxRedeliveries} повторов из очереди, событие отброшено");
                    return delivered;
                }

                int attempt = MaxAttempts;
                _journal.Write(_brokerId, $"RETRY из очереди: {evt.EventType} #{evt.EntityId} -> {channel.Name} ({queue.Count(channel.Name)} в очереди)");

                DeliveryOutcome outcome = await DeliverAsync(channel, evt, attempt);

                if (!outcome.Success)
                {
                    return delivered;
                }

                queue.Complete(channel.Name, evt.EventId);
                delivered++;
            }
        }
        finally
        {
            gate.Release();
        }
    }

    private SemaphoreSlim GateFor(string name)
    {
        lock (_sync)
        {
            if (!_gates.TryGetValue(name, out SemaphoreSlim? gate))
            {
                gate = new SemaphoreSlim(1, 1);
                _gates[name] = gate;
            }

            return gate;
        }
    }
}

/// <summary>Итог попытки доставки: успех или причина, по которой событие не обработано.</summary>
public class DeliveryOutcome
{
    public bool Success { get; set; }

    /// OK, UNAVAILABLE, ERROR, TIMEOUT, CONNECTION_LOST
    public string Status { get; set; } = "OK";

    public string Message { get; set; } = string.Empty;
    public int ProcessedMs { get; set; }
}
