using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Broker;
using Server;
using Shared.Events;
using Subscribers;
using Xunit;

namespace Tests;

/// <summary>
/// Живой Broker внутри теста: тот же accept-цикл и те же классы соединений, что у Broker.exe,
/// поэтому Publisher сервера и настоящий подписчик проверяются по сети, а не по заглушке.
/// </summary>
internal sealed class BrokerHost : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly EventJournal _journal;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _acceptLoop;

    private BrokerHost(EventBroker broker, EventJournal journal)
    {
        Broker = broker;
        _journal = journal;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoop = Task.Run(() => AcceptLoopAsync());
    }

    public EventBroker Broker { get; }

    public int Port { get; }

    public IReadOnlyList<string> JournalLines() => _journal.Recent(80);

    public static BrokerHost Start(string brokerId = "Broker-live", int deliveryTimeoutMs = 10000)
    {
        var journal = new EventJournal();
        var broker = new EventBroker(brokerId, journal, retryDelayMs: 20, deliveryTimeoutMs: deliveryTimeoutMs);

        return new BrokerHost(broker, journal);
    }

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                TcpClient client = await _listener.AcceptTcpClientAsync(_cts.Token);
                _ = Task.Run(() => ServeAsync(client));
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or SocketException or ObjectDisposedException)
        {
        }
    }

    private async Task ServeAsync(TcpClient client)
    {
        using (client)
        {
            try
            {
                LineConnection connection = LineConnection.FromAccepted(client);

                string? first = await connection.ReadLineAsync(_cts.Token);

                if (first == null)
                {
                    return;
                }

                PubSubMessage? envelope = PubSubJson.Deserialize<PubSubMessage>(first);
                HelloMessage? hello = envelope == null ? null : PubSubJson.Payload<HelloMessage>(envelope);

                if (hello == null || string.IsNullOrWhiteSpace(hello.Name))
                {
                    return;
                }

                if (hello.Kind.Equals("publisher", StringComparison.OrdinalIgnoreCase))
                {
                    await new PublisherConnection(connection, Broker, _journal, hello.Name).RunAsync(_cts.Token);
                }
                else
                {
                    await new SubscriberConnection(connection, Broker, _journal)
                        .RunAsync(hello.Name, hello.Reconnectable, _cts.Token);
                }
            }
            catch (Exception ex) when (ex is IOException or JsonException or SocketException or OperationCanceledException or PubSubTransportException)
            {
                // Тестовый хост не должен ронять тест из-за закрытого соединения.
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _listener.Stop();

        try
        {
            await _acceptLoop;
        }
        catch (Exception ex) when (ex is OperationCanceledException or SocketException or ObjectDisposedException)
        {
        }
    }
}

/// <summary>
/// Интеграционные тесты: Publisher сервера → Broker по TCP → настоящий подписчик.
/// Покрывают протокол, который в unit-тестах через заглушку не виден.
/// </summary>
public class PubSubIntegrationTests
{
    private readonly EventJournal _journal = new();
    private readonly Server.Journal _publisherJournal = new();

    [Fact]
    public async Task Publisher_Delivers_Event_To_Connected_Subscriber_Over_Tcp()
    {
        await using BrokerHost host = BrokerHost.Start();
        var publisher = new Publisher($"127.0.0.1:{host.Port}", _publisherJournal);
        var subscriber = new SubscriberInstance(
            "Logger", 0, new[] { EventTypes.RentalCreated }, reconnectable: true, _journal);

        await subscriber.ConnectAsync("127.0.0.1", host.Port);

        List<PublishResultMessage> results = publisher.Publish(new DomainEvent
        {
            EventType = EventTypes.RentalCreated,
            EntityId = 11,
            ClientId = 3
        });

        Assert.Equal("ACCEPTED", results[0].Status);
        Assert.Equal(1, results[0].SubscribersForEvent);
        Assert.True(results[0].PublishMs >= 0);

        await WaitUntilAsync(() => subscriber.Processed == 1);

        Assert.Equal(1, subscriber.Processed);
        Assert.Contains("EVENT RECEIVED RentalCreated", string.Join(" ", host.JournalLines()));
        Assert.Contains("ROUTING RentalCreated -> Logger", string.Join(" ", host.JournalLines()));
        Assert.Contains("PROCESS FINISH RentalCreated #11", string.Join(" ", _journal.Recent(80)));
    }

    [Fact]
    public async Task Publisher_Reports_Broker_Unavailable_And_Does_Not_Throw()
    {
        int deadPort;

        await using (BrokerHost host = BrokerHost.Start())
        {
            deadPort = host.Port;
        }

        var publisher = new Publisher($"127.0.0.1:{deadPort}", _publisherJournal);

        List<PublishResultMessage> results = publisher.Publish(new DomainEvent
        {
            EventType = EventTypes.RentalCreated,
            EntityId = 1
        });

        Assert.Equal("BROKER_UNAVAILABLE", results[0].Status);
        Assert.False(results[0].Success);
        Assert.Contains("BROKER UNAVAILABLE", string.Join(" ", _publisherJournal.Recent(20)));
    }

    [Fact]
    public async Task Publisher_Publishes_To_Two_Brokers_When_Second_Is_Down()
    {
        await using BrokerHost alive = BrokerHost.Start();
        int deadPort;

        await using (BrokerHost down = BrokerHost.Start())
        {
            deadPort = down.Port;
        }

        var subscriber = new SubscriberInstance(
            "Logger", 0, new[] { EventTypes.RentalCreated }, reconnectable: true, _journal);
        await subscriber.ConnectAsync("127.0.0.1", alive.Port);

        var publisher = new Publisher($"127.0.0.1:{alive.Port},127.0.0.1:{deadPort}", _publisherJournal);

        List<PublishResultMessage> results = publisher.Publish(new DomainEvent
        {
            EventType = EventTypes.RentalCreated,
            EntityId = 21
        });

        Assert.Equal(2, results.Count);
        Assert.Equal("ACCEPTED", results[0].Status);
        Assert.Equal("BROKER_UNAVAILABLE", results[1].Status);

        await WaitUntilAsync(() => subscriber.Processed == 1);
    }

    [Fact]
    public async Task Subscriber_Reconnect_Takes_Event_From_Queue()
    {
        await using BrokerHost host = BrokerHost.Start();
        var publisher = new Publisher($"127.0.0.1:{host.Port}", _publisherJournal);
        var subscriber = new SubscriberInstance(
            "CarService", 0, new[] { EventTypes.RentalCreated }, reconnectable: true, _journal);

        await subscriber.ConnectAsync("127.0.0.1", host.Port);

        subscriber.Mode = SubscriberMode.Unavailable;
        publisher.Publish(new DomainEvent { EventType = EventTypes.RentalCreated, EntityId = 31 });

        await WaitUntilAsync(() => host.Broker.Queue.Count("CarService") == 1);
        Assert.Equal(0, subscriber.Processed);

        await subscriber.DisconnectAsync("остановлен для проверки очереди");
        subscriber.Mode = SubscriberMode.Available;
        await subscriber.ConnectAsync("127.0.0.1", host.Port);

        // Событие уходит из очереди на шаг позже, чем подписчик считает обработку: ACK летит
        // асинхронно, и Broker снимает событие только получив его. Ждём оба факта.
        await WaitUntilAsync(() => subscriber.Processed == 1 && host.Broker.Queue.Count("CarService") == 0);

        Assert.Equal(0, host.Broker.Queue.Count("CarService"));
        Assert.Contains("QUEUED RentalCreated", string.Join(" ", host.JournalLines()));
        Assert.Contains("RETRY из очереди", string.Join(" ", host.JournalLines()));
    }

    [Fact]
    public async Task Late_Answer_Is_Not_Attributed_To_Another_Event()
    {
        await using BrokerHost host = BrokerHost.Start(deliveryTimeoutMs: 400);
        var publisher = new Publisher($"127.0.0.1:{host.Port}", _publisherJournal);
        using LineConnection subscriber = LineConnection.Connect("127.0.0.1", host.Port, connectTimeoutMs: 3000);

        await subscriber.SendAsync(PubSubJson.Message(PubSubOperations.Hello, new HelloMessage
        {
            Kind = "subscriber",
            Name = "Slow",
            Reconnectable = true
        }), CancellationToken.None);
        await subscriber.SendAsync(PubSubJson.Message(PubSubOperations.Subscribe, new SubscribeMessage
        {
            EventTypes = new List<string> { EventTypes.RentalCreated }
        }), CancellationToken.None);
        await ReadLineAsync(subscriber, "ответ на подписку");

        publisher.Publish(new DomainEvent { EventType = EventTypes.RentalCreated, EntityId = 41 });
        string firstId = EventIdOf(await ReadLineAsync(subscriber, "доставка первого события"));

        // Ответа на первое событие нет: Broker закрывает доставку по таймауту и оставляет событие в очереди.
        await Task.Delay(700);

        publisher.Publish(new DomainEvent { EventType = EventTypes.RentalCreated, EntityId = 42 });
        string secondId = EventIdOf(await ReadLineAsync(subscriber, "доставка второго события"));

        // Запоздалый ответ по первому событию приходит, когда Broker ждёт ответ уже по второму.
        await subscriber.SendAsync(PubSubJson.Message(PubSubOperations.Result, new DeliveryResultMessage
        {
            Success = true,
            Status = "OK",
            EventId = firstId
        }), CancellationToken.None);
        await Task.Delay(150);

        Assert.Contains("СТАРЫЙ ACK", string.Join(" ", host.JournalLines()));
        Assert.DoesNotContain("Slow -> SUCCESS", string.Join(" ", host.JournalLines()));
        Assert.Equal(1, host.Broker.Queue.Count("Slow"));

        await subscriber.SendAsync(PubSubJson.Message(PubSubOperations.Result, new DeliveryResultMessage
        {
            Success = true,
            Status = "OK",
            EventId = secondId
        }), CancellationToken.None);

        await WaitUntilAsync(() => host.JournalLines().Any(line => line.Contains("Slow -> SUCCESS")));
        Assert.Equal(1, host.Broker.Queue.Count("Slow"));
    }

    [Fact]
    public async Task Broker_Status_Is_Visible_Through_Publisher()
    {
        await using BrokerHost host = BrokerHost.Start();
        var publisher = new Publisher($"127.0.0.1:{host.Port}", _publisherJournal);
        var subscriber = new SubscriberInstance(
            "Logger", 0, new[] { EventTypes.RentalCreated, EventTypes.CarReturned }, reconnectable: true, _journal);

        await subscriber.ConnectAsync("127.0.0.1", host.Port);

        List<BrokerInfoResultMessage> info = publisher.GetBrokersInfo();

        BrokerInfoResultMessage broker = Assert.Single(info);
        Assert.True(broker.Success);
        Assert.Equal("READY", broker.Status);
        Assert.Equal(1, broker.OnlineSubscribers);
        Assert.Contains(broker.Subscriptions, line => line.StartsWith("Logger:"));
    }

    [Fact]
    public async Task Subscriber_Can_Subscribe_To_Unknown_Event_Type_And_Broker_Rejects_It()
    {
        await using BrokerHost host = BrokerHost.Start();
        var subscriber = new SubscriberInstance("Logger", 0, Array.Empty<string>(), reconnectable: true, _journal);
        await subscriber.ConnectAsync("127.0.0.1", host.Port);

        SubscribeResultMessage result = await subscriber.SubscribeAsync(new[] { "PaymentCompleted" });

        Assert.False(result.Success);
        Assert.Equal("UNKNOWN_EVENT_TYPE", result.Status);
    }

    [Fact]
    public async Task Subscribers_Filter_Only_Their_Own_Event_Types_Over_Tcp()
    {
        await using BrokerHost host = BrokerHost.Start();
        var publisher = new Publisher($"127.0.0.1:{host.Port}", _publisherJournal);

        var logger = new SubscriberInstance("Logger", 0,
            new[] { EventTypes.RentalCreated, EventTypes.RentalCancelled, EventTypes.CarReturned }, true, _journal);
        var carService = new SubscriberInstance("CarService", 0,
            new[] { EventTypes.RentalCreated }, true, _journal);

        await logger.ConnectAsync("127.0.0.1", host.Port);
        await carService.ConnectAsync("127.0.0.1", host.Port);

        publisher.Publish(new DomainEvent { EventType = EventTypes.RentalCreated, EntityId = 41 });
        await WaitUntilAsync(() => carService.Processed == 1);

        publisher.Publish(new DomainEvent { EventType = EventTypes.CarReturned, EntityId = 41 });
        await WaitUntilAsync(() => logger.Processed == 2);

        Assert.Equal(2, logger.Processed);
        Assert.Equal(1, carService.Processed);
    }

    /// Строка протокола от Broker с таймаутом: без него тест висит, если доставка не пришла.
    private static async Task<string> ReadLineAsync(LineConnection connection, string what)
    {
        using var timeout = new CancellationTokenSource(3000);

        try
        {
            string? line = await connection.ReadLineAsync(timeout.Token);
            return line ?? throw new TimeoutException($"{what}: соединение закрыто");
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException($"{what}: не пришло за 3000 мс");
        }
    }

    private static string EventIdOf(string deliveryLine)
    {
        PubSubMessage? envelope = PubSubJson.Deserialize<PubSubMessage>(deliveryLine);
        DeliveryMessage? delivery = envelope == null ? null : PubSubJson.Payload<DeliveryMessage>(envelope);

        return delivery?.Event?.EventId ?? string.Empty;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 4000)
    {
        var watch = System.Diagnostics.Stopwatch.StartNew();

        while (!condition())
        {
            if (watch.ElapsedMilliseconds > timeoutMs)
            {
                throw new TimeoutException("условие не наступило за отведённое время");
            }

            await Task.Delay(20);
        }
    }
}
