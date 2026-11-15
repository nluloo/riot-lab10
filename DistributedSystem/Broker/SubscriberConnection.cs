using System.Text.Json;
using Shared.Events;

namespace Broker;

/// <summary>
/// Соединение Broker с одним подписчиком. Читатель здесь один на весь поток:
/// и ответы на доставку, и команды подписки приходят в один цикл чтения.
/// </summary>
public class SubscriberConnection
{
    private readonly object _sync = new();
    private readonly LineConnection _connection;
    private readonly EventBroker _broker;
    private readonly EventJournal _journal;

    private TaskCompletionSource<DeliveryResultMessage>? _pendingAck;
    private string _pendingEventId = string.Empty;

    public SubscriberConnection(LineConnection connection, EventBroker broker, EventJournal journal)
    {
        _connection = connection;
        _broker = broker;
        _journal = journal;
    }

    public SubscriberChannel? Channel { get; private set; }

    /// Основной цикл: подписка, отмена подписки, состояние и ответы подписчика на доставку.
    public async Task RunAsync(string name, bool reconnectable, CancellationToken cancellationToken)
    {
        Channel = _broker.AcceptConnection(name, reconnectable);
        Channel.Send = SendDeliveryAsync;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
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
                    _journal.Write(_broker.BrokerId, $"НЕКОРРЕКТНЫЙ JSON от {name}: {Short(line)}");
                    continue;
                }

                if (message == null)
                {
                    continue;
                }

                if (message.Operation == PubSubOperations.Result)
                {
                    CompleteDelivery(message);
                    continue;
                }

                await HandleControlAsync(message, cancellationToken);
            }
        }
        finally
        {
            _broker.CloseConnection(Channel);
        }
    }

    private async Task<DeliveryResultMessage> SendDeliveryAsync(DeliveryMessage message, CancellationToken cancellationToken)
    {
        var source = new TaskCompletionSource<DeliveryResultMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Доставки одному подписчику сериализованы семафором в DeliveryManager,
        // поэтому ожидающий ответ на соединении всегда ровно один.
        lock (_sync)
        {
            _pendingAck = source;
            _pendingEventId = message.Event?.EventId ?? string.Empty;
        }

        try
        {
            await _connection.SendAsync(PubSubJson.Message(PubSubOperations.Deliver, message), cancellationToken);

            using (cancellationToken.Register(() => source.TrySetCanceled(cancellationToken)))
            {
                return await source.Task;
            }
        }
        finally
        {
            lock (_sync)
            {
                if (_pendingAck == source)
                {
                    _pendingAck = null;
                    _pendingEventId = string.Empty;
                }
            }
        }
    }

    private void CompleteDelivery(PubSubMessage message)
    {
        DeliveryResultMessage? ack = PubSubJson.Payload<DeliveryResultMessage>(message)
            ?? new DeliveryResultMessage { Success = false, Status = "ERROR", Message = "пустой ответ подписчика" };

        TaskCompletionSource<DeliveryResultMessage>? waiter;
        string waitingFor;

        lock (_sync)
        {
            waiter = _pendingAck;
            waitingFor = _pendingEventId;
        }

        if (waiter == null)
        {
            return;
        }

        // Подписчик мог ответить уже после того, как Broker отменил доставку по таймауту. Такой ответ
        // относится к прежнему событию: принимать его за текущую доставку нельзя — результат будет выдуманный.
        if (!string.IsNullOrEmpty(ack.EventId) && ack.EventId != waitingFor)
        {
            _journal.Write(_broker.BrokerId,
                $"СТАРЫЙ ACK {ack.EventId} от {Channel?.Name}: доставка по нему уже закрыта, ответ текущей не отдаём");
            return;
        }

        waiter.TrySetResult(ack);
    }

    private async Task HandleControlAsync(PubSubMessage message, CancellationToken cancellationToken)
    {
        switch (message.Operation)
        {
            case PubSubOperations.Subscribe:
            {
                SubscribeMessage? data = PubSubJson.Payload<SubscribeMessage>(message);

                if (data == null || Channel == null)
                {
                    await SendResultAsync(new SubscribeResultMessage
                    {
                        Status = "NO_CONNECTION",
                        Message = "Сообщение подписки получено без соединения с подписчиком"
                    }, cancellationToken);

                    return;
                }

                SubscriptionResult result = await _broker.SubscribeAsync(Channel, data.EventTypes);

                await SendResultAsync(new SubscribeResultMessage
                {
                    Success = result.Success,
                    Status = result.Status,
                    Message = result.Message,
                    Types = result.Types
                }, cancellationToken);

                return;
            }

            case PubSubOperations.Unsubscribe:
            {
                if (Channel == null)
                {
                    return;
                }

                IReadOnlyList<string> removed = _broker.Unsubscribe(Channel);

                await SendResultAsync(new SubscribeResultMessage
                {
                    Success = true,
                    Status = "OK",
                    Message = removed.Count > 0 ? $"снято подписок: {string.Join(", ", removed)}" : "подписок не было",
                    Types = removed.ToList()
                }, cancellationToken);

                return;
            }

            case PubSubOperations.Info:
                await SendResultAsync(_broker.GetInfo(), cancellationToken);
                return;

            default:
                _journal.Write(_broker.BrokerId, $"НЕИЗВЕСТНАЯ ОПЕРАЦИЯ '{message.Operation}' от {Channel?.Name}");
                return;
        }
    }

    private Task SendResultAsync(object data, CancellationToken cancellationToken) =>
        _connection.SendAsync(PubSubJson.Message(PubSubOperations.Result, data), cancellationToken);

    private static string Short(string line) => line.Length <= 120 ? line : line[..120] + "…";
}
