using System.Diagnostics;
using System.Text.Json;
using Shared.Events;

namespace Server;

/// <summary>
/// Publisher: принимает событие от сервера, передаёт его Broker'у и фиксирует результат публикации.
/// К Subscriber он не обращается, не знает их количества и не участвует в маршрутизации.
/// </summary>
public class Publisher
{
    private const int ConnectTimeoutMs = 1000;
    private const int RequestTimeoutMs = 2000;

    private readonly List<BrokerEndpoint> _endpoints;
    private readonly Journal _journal;

    public Publisher(string endpoints, Journal journal)
    {
        _journal = journal;
        _endpoints = ParseEndpoints(endpoints);
    }

    public IReadOnlyList<BrokerEndpoint> Endpoints => _endpoints;

    /// Публикация во все Broker. Отказ одного — строка в журнале и понятный результат Publisher'у:
    /// данные операции уже сохранены, поэтому запрос клиента от публикации не откатывается.
    public List<PublishResultMessage> Publish(DomainEvent evt)
    {
        var results = new List<PublishResultMessage>();

        foreach (BrokerEndpoint endpoint in _endpoints)
        {
            results.Add(PublishTo(endpoint, evt));
        }

        return results;
    }

    public List<BrokerInfoResultMessage> GetBrokersInfo()
    {
        var results = new List<BrokerInfoResultMessage>();

        foreach (BrokerEndpoint endpoint in _endpoints)
        {
            PubSubMessage? reply = Request(endpoint, PubSubOperations.Info, new { });
            BrokerInfoResultMessage? info = reply == null ? null : PubSubJson.Payload<BrokerInfoResultMessage>(reply);

            results.Add(info ?? new BrokerInfoResultMessage
            {
                BrokerId = endpoint.Label,
                Status = "BROKER_UNAVAILABLE",
                Message = "Broker не ответил на запрос состояния"
            });
        }

        return results;
    }

    /// Короткая строка о судьбе события — её сервер отдаёт клиенту в Response.Events.
    public static string Describe(DomainEvent evt, List<PublishResultMessage> results)
    {
        string parts = string.Join("; ", results.Select(result =>
            $"{result.BrokerId}: {result.Status}" +
            (result.SubscribersForEvent > 0 ? $", получателей {result.SubscribersForEvent}" : string.Empty) +
            (result.Success ? string.Empty : $", {result.Message}")));

        return $"{evt.EventType} #{evt.EntityId} (event {evt.EventId}) -> {parts}";
    }

    private PublishResultMessage PublishTo(BrokerEndpoint endpoint, DomainEvent evt)
    {
        var watch = Stopwatch.StartNew();

        PubSubMessage? reply = Request(endpoint, PubSubOperations.Publish, evt);
        PublishResultMessage? result = reply == null ? null : PubSubJson.Payload<PublishResultMessage>(reply);

        if (result == null)
        {
            return new PublishResultMessage
            {
                BrokerId = endpoint.Label,
                Status = "BROKER_UNAVAILABLE",
                Message = "Broker не принял событие",
                PublishMs = (int)watch.ElapsedMilliseconds
            };
        }

        result.PublishMs = (int)watch.ElapsedMilliseconds;

        if (string.IsNullOrWhiteSpace(result.BrokerId))
        {
            result.BrokerId = endpoint.Label;
        }

        return result;
    }

    /// Один запрос к Broker на отдельном соединении. Исключений наружу не отдаёт: возвращает null.
    private PubSubMessage? Request(BrokerEndpoint endpoint, string operation, object data)
    {
        var watch = Stopwatch.StartNew();

        try
        {
            using LineConnection connection = LineConnection.Connect(endpoint.Host, endpoint.Port, ConnectTimeoutMs);

            connection.SendSync(
                PubSubJson.Message(PubSubOperations.Hello, new HelloMessage { Kind = "publisher", Name = "Server" }),
                RequestTimeoutMs);

            connection.SendSync(PubSubJson.Message(operation, data), RequestTimeoutMs);

            string? line = connection.ReadLineSync(RequestTimeoutMs);

            if (line == null)
            {
                _journal.Write("Publisher", $"{operation} -> {endpoint.Label}: BROKER UNAVAILABLE, Broker закрыл соединение ({watch.ElapsedMilliseconds} мс)");
                return null;
            }

            PubSubMessage? message;

            try
            {
                message = PubSubJson.Deserialize<PubSubMessage>(line);
            }
            catch (JsonException)
            {
                _journal.Write("Publisher", $"{operation} -> {endpoint.Label}: некорректный JSON в ответе Broker");
                return null;
            }

            if (message == null)
            {
                _journal.Write("Publisher", $"{operation} -> {endpoint.Label}: пустой ответ Broker");
                return null;
            }

            _journal.Write("Publisher", $"{operation} -> {endpoint.Label}: ответ за {watch.ElapsedMilliseconds} мс");
            return message;
        }
        catch (PubSubTransportException ex)
        {
            _journal.Write("Publisher",
                $"{operation} -> {endpoint.Label}: BROKER UNAVAILABLE ({watch.ElapsedMilliseconds} мс) — {ex.Message}. Запрос клиента от этого не отменяется.");

            return null;
        }
    }

    private List<BrokerEndpoint> ParseEndpoints(string endpoints)
    {
        var parsed = new List<BrokerEndpoint>();

        foreach (string raw in (endpoints ?? string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            string[] parts = raw.Split(':', 2);

            if (parts.Length != 2 || !int.TryParse(parts[1], out int port) || port <= 0 || port > 65535)
            {
                _journal.Write("Publisher", $"адрес Broker '{raw}' некорректен — пропущен");
                continue;
            }

            parsed.Add(new BrokerEndpoint(parts[0].Trim(), port));
        }

        if (parsed.Count == 0)
        {
            parsed.Add(new BrokerEndpoint("127.0.0.1", 5001));
            _journal.Write("Publisher", "ни один адрес Broker не разобран, использован 127.0.0.1:5001");
        }

        return parsed;
    }
}

/// <summary>Адрес Broker'а: хост и порт TCP.</summary>
public class BrokerEndpoint
{
    public BrokerEndpoint(string host, int port)
    {
        Host = host;
        Port = port;
    }

    public string Host { get; }

    public int Port { get; }

    public string Label => $"{Host}:{Port}";
}
