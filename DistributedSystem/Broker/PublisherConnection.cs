using System.Text.Json;
using Shared.Events;

namespace Broker;

/// <summary>
/// Соединение Broker с Publisher. Publisher обращается только сюда и не знает,
/// сколько подписчиков сейчас на связи и что они умеют.
/// </summary>
public class PublisherConnection
{
    private readonly LineConnection _connection;
    private readonly EventBroker _broker;
    private readonly EventJournal _journal;
    private readonly string _publisherName;

    public PublisherConnection(LineConnection connection, EventBroker broker, EventJournal journal, string publisherName)
    {
        _connection = connection;
        _broker = broker;
        _journal = journal;
        _publisherName = string.IsNullOrWhiteSpace(publisherName) ? "Publisher" : publisherName;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _journal.Write(_broker.BrokerId, $"PUBLISHER CONNECTED {_publisherName}");

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
                    _journal.Write(_broker.BrokerId, $"НЕКОРРЕКТНЫЙ JSON от {_publisherName}: {Short(line)}");
                    await SendResultAsync(new PublishResultMessage
                    {
                        BrokerId = _broker.BrokerId,
                        Status = "INVALID_EVENT",
                        Message = "Некорректный JSON в запросе публикации"
                    }, cancellationToken);

                    continue;
                }

                if (message == null)
                {
                    continue;
                }

                if (message.Operation == PubSubOperations.Info)
                {
                    await SendResultAsync(_broker.GetInfo(), cancellationToken);
                    continue;
                }

                if (message.Operation != PubSubOperations.Publish)
                {
                    _journal.Write(_broker.BrokerId, $"НЕИЗВЕСТНАЯ ОПЕРАЦИЯ '{message.Operation}' от {_publisherName}");

                    await SendResultAsync(new PublishResultMessage
                    {
                        BrokerId = _broker.BrokerId,
                        Status = "INVALID_EVENT",
                        Message = $"Ожидалась публикация, получена операция '{message.Operation}'"
                    }, cancellationToken);

                    continue;
                }

                DomainEvent? evt = PubSubJson.Payload<DomainEvent>(message);

                PublishResultMessage result = evt == null
                    ? new PublishResultMessage
                    {
                        BrokerId = _broker.BrokerId,
                        Status = "INVALID_EVENT",
                        Message = "Пустое событие: публиковать нечего"
                    }
                    : await _broker.PublishAsync(evt);

                await SendResultAsync(result, cancellationToken);
            }
        }
        finally
        {
            _journal.Write(_broker.BrokerId, $"PUBLISHER DISCONNECTED {_publisherName}");
        }
    }

    private Task SendResultAsync(object data, CancellationToken cancellationToken) =>
        _connection.SendAsync(PubSubJson.Message(PubSubOperations.Result, data), cancellationToken);

    private static string Short(string line) => line.Length <= 120 ? line : line[..120] + "…";
}
