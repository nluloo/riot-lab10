using System.Text.Json;
using Shared.Events;

namespace Subscribers;

/// <summary>Способ, которым подписчик «портит» доставку — для Части 3 и Части 5.</summary>
public enum SubscriberMode
{
    Available,

    /// Отказывается обрабатывать: Broker видит UNAVAILABLE.
    Unavailable,

    /// Дважды отвечает ошибкой, на третий раз справляется — видны Attempt 1 FAIL, 2 FAIL, 3 SUCCESS.
    FailingTwice,

    /// Молчит дольше таймаута Broker — виден TIMEOUT.
    NoAnswer
}

/// <summary>
/// Один подписчик: собственное TCP-соединение с Broker, свои подписки, своя задержка обработки.
/// Отдельные соединения — поэтому отказ одного подписчика реально рвёт только его канал, а не весь процесс.
/// </summary>
public class SubscriberInstance : IDisposable
{
    /// Молчание в режиме NO ANSWER. Заведомо короче дедлайна доставки у Broker (10 с),
    /// чтобы подписчик успел сам ответить «TIMEOUT» и не плохить следующую доставку.
    private const int NoAnswerMs = 5000;
    private const int RequestTimeoutMs = 5000;

    private readonly EventJournal _journal;
    private readonly List<string> _received = new();
    private readonly Dictionary<string, int> _counts = new();
    private readonly object _sync = new();

    private LineConnection? _connection;
    private CancellationTokenSource? _cts;
    private TaskCompletionSource<PubSubMessage>? _pendingRequest;
    private int _failures;

    public SubscriberInstance(
        string name,
        int delayMs,
        IEnumerable<string> eventTypes,
        bool reconnectable,
        EventJournal journal)
    {
        Name = name;
        DelayMs = delayMs;
        Subscriptions = eventTypes.ToList();
        Reconnectable = reconnectable;
        _journal = journal;
    }

    public string Name { get; }

    public int DelayMs { get; set; }

    /// false — Broker не копит для этого подписчика события: что не доставил, то потеряно.
    public bool Reconnectable { get; }

    public SubscriberMode Mode { get; set; } = SubscriberMode.Available;

    /// Пусто — Broker не доставит этому подписчику ничего.
    public IReadOnlyList<string> Subscriptions { get; private set; }

    public bool IsConnected => _connection != null;

    public string ModeText => Mode switch
    {
        SubscriberMode.Available => "AVAILABLE",
        SubscriberMode.Unavailable => "UNAVAILABLE",
        SubscriberMode.FailingTwice => "FAILING (2 попытки)",
        SubscriberMode.NoAnswer => "NO ANSWER",
        _ => "UNKNOWN"
    };

    public IReadOnlyList<string> ReceivedLines
    {
        get
        {
            lock (_sync)
            {
                return _received.ToList();
            }
        }
    }

    public IReadOnlyDictionary<string, int> CountsByType
    {
        get
        {
            lock (_sync)
            {
                return _counts.ToDictionary(pair => pair.Key, pair => pair.Value);
            }
        }
    }

    public int Processed { get; private set; }

    public int Failed { get; private set; }

    public int LastProcessedMs { get; private set; }

    public async Task ConnectAsync(string host, int port)
    {
        if (IsConnected)
        {
            _journal.Write(Name, "SUBSCRIBE FAIL: соединение с Broker уже открыто");
            return;
        }

        _connection = LineConnection.Connect(host, port, connectTimeoutMs: 3000);
        _cts = new CancellationTokenSource();

        _journal.Write(Name, $"CONNECT → {host}:{port}, задержка обработки {DelayMs} мс, " +
                             $"подписки: {(Subscriptions.Count > 0 ? string.Join(", ", Subscriptions) : "нет")}, " +
                             $"{(Reconnectable ? "очередь в Broker разрешена" : "очередь не ведётся")}");

        await _connection.SendAsync(
            PubSubJson.Message(PubSubOperations.Hello, new HelloMessage
            {
                Kind = "subscriber",
                Name = Name,
                Reconnectable = Reconnectable
            }),
            CancellationToken.None);

        _ = Task.Run(() => ReadLoopAsync(_cts.Token));

        if (Subscriptions.Count > 0)
        {
            await SubscribeAsync(Subscriptions);
        }
    }

    /// Остановка подписчика: соединение реально закрывается, Broker видит обрыв и перестаёт ему доставлять.
    public Task DisconnectAsync(string reason)
    {
        if (!IsConnected)
        {
            _journal.Write(Name, $"STOP FAIL: подписчик уже остановлен ({reason})");
            return Task.CompletedTask;
        }

        CancellationTokenSource? cts = _cts;
        LineConnection? connection = _connection;
        _connection = null;
        _cts = null;

        cts?.Cancel();
        connection?.Dispose();

        _journal.Write(Name, $"STOPPED ({reason}) — соединение с Broker закрыто");
        return Task.CompletedTask;
    }

    public async Task<SubscribeResultMessage> SubscribeAsync(IEnumerable<string> eventTypes)
    {
        List<string> types = eventTypes.ToList();

        PubSubMessage reply = await RequestAsync(
            PubSubOperations.Subscribe,
            new SubscribeMessage { Name = Name, EventTypes = types });

        SubscribeResultMessage? result = PubSubJson.Payload<SubscribeResultMessage>(reply);

        if (result != null && result.Success)
        {
            Subscriptions = result.Types;
        }

        _journal.Write(Name, result == null
            ? "SUBSCRIBE FAIL: Broker не ответил"
            : $"SUBSCRIBE [{string.Join(", ", types)}] -> {result.Status}: {result.Message}");

        return result ?? new SubscribeResultMessage { Status = "NO_CONNECTION", Message = "Broker не ответил" };
    }

    public async Task<SubscribeResultMessage> UnsubscribeAsync()
    {
        PubSubMessage reply = await RequestAsync(
            PubSubOperations.Unsubscribe,
            new UnsubscribeMessage { Name = Name });

        SubscribeResultMessage? result = PubSubJson.Payload<SubscribeResultMessage>(reply);

        if (result != null && result.Success)
        {
            Subscriptions = Array.Empty<string>();
        }

        _journal.Write(Name, $"UNSUBSCRIBE -> {result?.Status ?? "NO_CONNECTION"}: {result?.Message ?? "Broker не ответил"}");

        return result ?? new SubscribeResultMessage { Status = "NO_CONNECTION", Message = "Broker не ответил" };
    }

    public async Task<BrokerInfoResultMessage?> GetBrokerInfoAsync()
    {
        PubSubMessage reply = await RequestAsync(PubSubOperations.Info, new { });

        return PubSubJson.Payload<BrokerInfoResultMessage>(reply);
    }

    /// Единственный читатель соединения: доставку обрабатывает, ответы Broker передаёт ожидающему запросу.
    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested && _connection != null)
            {
                string? line = await _connection.ReadLineAsync(cancellationToken);

                if (line == null)
                {
                    break;
                }

                PubSubMessage? message;

                try
                {
                    message = PubSubJson.Deserialize<PubSubMessage>(line);
                }
                catch (JsonException)
                {
                    _journal.Write(Name, "получен некорректный JSON от Broker — сообщение пропущено");
                    continue;
                }

                if (message == null)
                {
                    continue;
                }

                if (message.Operation == PubSubOperations.Deliver)
                {
                    await HandleDeliveryAsync(message, cancellationToken);
                    continue;
                }

                TaskCompletionSource<PubSubMessage>? waiter;

                lock (_sync)
                {
                    waiter = _pendingRequest;
                }

                waiter?.TrySetResult(message);
            }
        }
        catch (IOException)
        {
            // Соединение закрыто намеренно или Broker недоступен — разберёт RequestAsync.
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            _journal.Write(Name, $"ошибка чтения от Broker: {ex.Message}");
        }
        finally
        {
            _connection = null;

            TaskCompletionSource<PubSubMessage>? waiter;

            lock (_sync)
            {
                waiter = _pendingRequest;
                _pendingRequest = null;
            }

            waiter?.TrySetCanceled();
        }
    }

    private async Task HandleDeliveryAsync(PubSubMessage message, CancellationToken cancellationToken)
    {
        DeliveryMessage? delivery = PubSubJson.Payload<DeliveryMessage>(message);
        DomainEvent? evt = delivery?.Event;

        if (evt == null)
        {
            _journal.Write(Name, "получено событие без данных — отвечено ошибкой");
            await AnswerAsync(new DeliveryResultMessage
            {
                Status = "ERROR",
                Message = "подписчик получил пустое событие"
            }, cancellationToken);

            return;
        }

        DeliveryResultMessage result = await ProcessAsync(evt, delivery!.Attempt);
        result.EventId = evt.EventId;

        await AnswerAsync(result, cancellationToken);
    }

    /// Собственно обработка события подписчиком. Исключений наружу не отдаёт: отказ — это результат.
    public async Task<DeliveryResultMessage> ProcessAsync(DomainEvent evt, int attempt)
    {
        string what = $"{evt.EventType} #{evt.EntityId} (клиент {evt.ClientId})";

        if (Mode == SubscriberMode.Unavailable)
        {
            Failed++;
            _journal.Write(Name, $"SKIP {what}: подписчик намеренно недоступен");

            return new DeliveryResultMessage
            {
                Status = "UNAVAILABLE",
                Message = "подписчик переведён в режим UNAVAILABLE"
            };
        }

        if (Mode == SubscriberMode.NoAnswer)
        {
            _journal.Write(Name, $"NO ANSWER {what}: подписчик молчит {NoAnswerMs} мс (Broker должен зафиксировать TIMEOUT)");
            await Task.Delay(NoAnswerMs);
            Failed++;

            return new DeliveryResultMessage
            {
                Status = "TIMEOUT",
                Message = "ответ не отправлен в срок"
            };
        }

        if (Mode == SubscriberMode.FailingTwice && _failures < 2)
        {
            _failures++;
            Failed++;
            _journal.Write(Name, $"ERROR {what}: искусственная ошибка обработки (попытка {attempt})");

            return new DeliveryResultMessage
            {
                Status = "ERROR",
                Message = $"искусственная ошибка обработки (попытка {_failures} из 2)"
            };
        }

        _failures = 0;

        var watch = System.Diagnostics.Stopwatch.StartNew();
        _journal.Write(Name, $"PROCESS START {what}, задержка {DelayMs} мс");
        await Task.Delay(DelayMs);

        lock (_sync)
        {
            _received.Insert(0, $"{what} attempt {attempt} за {watch.ElapsedMilliseconds} мс");

            while (_received.Count > 30)
            {
                _received.RemoveAt(_received.Count - 1);
            }

            _counts.TryGetValue(evt.EventType, out int known);
            _counts[evt.EventType] = known + 1;
        }

        LastProcessedMs = (int)watch.ElapsedMilliseconds;
        Processed++;
        ApplyDomainEffect(evt);
        _journal.Write(Name, $"PROCESS FINISH {what} за {LastProcessedMs} мс");

        return new DeliveryResultMessage
        {
            Success = true,
            Status = "OK",
            Message = $"обработано за {LastProcessedMs} мс",
            ProcessedMs = LastProcessedMs
        };
    }

    /// Что именно делает подписчик с предметной областью — только свои локальные записи, БД он не трогает.
    private void ApplyDomainEffect(DomainEvent evt)
    {
        switch (evt.EventType)
        {
            case EventTypes.RentalCreated:
                _journal.Write(Name, $"учёт: аренда #{evt.EntityId} открыта");
                break;
            case EventTypes.RentalCancelled:
                _journal.Write(Name, $"учёт: аренда #{evt.EntityId} отменена");
                break;
            case EventTypes.CarReturned:
                _journal.Write(Name, $"учёт: аренда #{evt.EntityId} закрыта, автомобиль возвращён");
                break;
        }
    }

    private async Task<PubSubMessage> RequestAsync(string operation, object data)
    {
        if (_connection == null)
        {
            _journal.Write(Name, $"REQUEST FAIL '{operation}': нет соединения с Broker");

            return PubSubJson.Message(PubSubOperations.Result, new SubscribeResultMessage
            {
                Status = "NO_CONNECTION",
                Message = $"подписчик '{Name}' не подключён к Broker"
            });
        }

        var source = new TaskCompletionSource<PubSubMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        lock (_sync)
        {
            _pendingRequest = source;
        }

        try
        {
            await _connection.SendAsync(PubSubJson.Message(operation, data), CancellationToken.None);
            return await source.Task.WaitAsync(TimeSpan.FromMilliseconds(RequestTimeoutMs));
        }
        catch (TimeoutException)
        {
            _journal.Write(Name, $"REQUEST FAIL '{operation}': Broker не ответил за {RequestTimeoutMs} мс");

            return PubSubJson.Message(PubSubOperations.Result, new SubscribeResultMessage
            {
                Status = "NO_CONNECTION",
                Message = "Broker не ответил на запрос"
            });
        }
        catch (Exception ex) when (ex is IOException or OperationCanceledException or PubSubTransportException)
        {
            _journal.Write(Name, $"REQUEST FAIL '{operation}': {ex.Message}");

            return PubSubJson.Message(PubSubOperations.Result, new SubscribeResultMessage
            {
                Status = "NO_CONNECTION",
                Message = "соединение с Broker потеряно"
            });
        }
        finally
        {
            lock (_sync)
            {
                if (_pendingRequest == source)
                {
                    _pendingRequest = null;
                }
            }
        }
    }

    private async Task AnswerAsync(DeliveryResultMessage result, CancellationToken cancellationToken)
    {
        LineConnection? connection = _connection;

        if (connection == null)
        {
            return;
        }

        try
        {
            await connection.SendAsync(PubSubJson.Message(PubSubOperations.Result, result), cancellationToken);
        }
        catch (Exception ex) when (ex is IOException or OperationCanceledException or PubSubTransportException)
        {
            _journal.Write(Name, $"не удалось ответить Broker: {ex.Message}");
        }
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _connection?.Dispose();
        _connection = null;
    }
}
