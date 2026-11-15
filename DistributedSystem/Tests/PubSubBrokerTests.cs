using Broker;
using Shared.Events;
using Xunit;

namespace Tests;

/// <summary>
/// Тесты механизма Pub/Sub из Части 1–4: создание события, подписки, маршрутизация,
/// доставка нескольким подписчикам, отказ одного, очередь и повторная доставка.
/// </summary>
public class PubSubBrokerTests
{
    // Требование 2 Части 1.
    [Fact]
    public void Event_Carries_Type_Entity_Timestamp_And_Data()
    {
        var evt = new DomainEvent
        {
            EventType = EventTypes.RentalCreated,
            EntityId = 125,
            ClientId = 7,
            Data = new Dictionary<string, object?> { ["registrationNumber"] = "1234 AB-7" }
        };

        Assert.Equal("RentalCreated", evt.EventType);
        Assert.Equal(125, evt.EntityId);
        Assert.NotEqual(default, evt.Timestamp);
        Assert.Equal("1234 AB-7", evt.Data["registrationNumber"]);
        Assert.NotEqual(string.Empty, evt.EventId);
    }

    // Требование 3 и 4 Части 1.
    [Fact]
    public async Task Publish_Accepts_Event_And_Counts_Matching_Subscribers()
    {
        var rig = new PubSubRig();
        rig.Add("Logger", EventTypes.RentalCreated);
        rig.Add("CarService", EventTypes.CarReturned);

        PublishResultMessage result = await rig.PublishAsync(EventTypes.RentalCreated);

        Assert.True(result.Success);
        Assert.Equal("ACCEPTED", result.Status);
        Assert.Equal(1, result.SubscribersForEvent);
        Assert.Equal(1, result.DeliveredCount);
    }

    // Требование 5 Части 1.
    [Fact]
    public void Subscribe_Registers_Event_Type_For_Subscriber()
    {
        var rig = new PubSubRig();
        rig.Add("Logger");

        SubscriptionResult result = rig.Broker.Subscriptions.Subscribe("Logger", new[] { EventTypes.RentalCreated });

        Assert.True(result.Success);
        Assert.Equal("OK", result.Status);
        Assert.Contains(EventTypes.RentalCreated, rig.Broker.Subscriptions.EventTypesOf("Logger"));
    }

    // Требование 6 Части 1.
    [Fact]
    public async Task Subscribed_Subscriber_Receives_Event()
    {
        var rig = new PubSubRig();
        FakeSubscriber logger = rig.Add("Logger", EventTypes.RentalCreated);

        await rig.PublishAsync(EventTypes.RentalCreated, entityId: 42);

        DomainEvent received = Assert.Single(logger.Received);
        Assert.Equal(42, received.EntityId);
        Assert.Equal(EventTypes.RentalCreated, received.EventType);
    }

    // Требование 4 Части 2.
    [Fact]
    public async Task Subscriber_Without_Subscription_Does_Not_Get_Event()
    {
        var rig = new PubSubRig();
        FakeSubscriber carService = rig.Add("CarService", EventTypes.CarReturned);

        await rig.PublishAsync(EventTypes.RentalCreated);

        Assert.Empty(carService.Received);
        Assert.Equal(0, carService.Processed);
    }

    // Требование 3 Части 2.
    [Fact]
    public async Task One_Event_Goes_To_Every_Subscribed_Subscriber()
    {
        var rig = new PubSubRig();
        FakeSubscriber logger = rig.Add("Logger", EventTypes.RentalCreated);
        FakeSubscriber carService = rig.Add("CarService", EventTypes.RentalCreated);
        FakeSubscriber notifications = rig.Add("NotificationService", EventTypes.RentalCreated);

        PublishResultMessage result = await rig.PublishAsync(EventTypes.RentalCreated);

        Assert.Equal(3, result.SubscribersForEvent);
        Assert.Equal(3, result.DeliveredCount);
        Assert.Single(logger.Received);
        Assert.Single(carService.Received);
        Assert.Single(notifications.Received);
    }

    // Требование 4 Части 2.
    [Fact]
    public async Task Different_Event_Types_Route_To_Different_Subscribers()
    {
        var rig = new PubSubRig();
        FakeSubscriber logger = rig.Add("Logger", EventTypes.RentalCreated, EventTypes.RentalCancelled, EventTypes.CarReturned);
        FakeSubscriber carService = rig.Add("CarService", EventTypes.RentalCreated, EventTypes.CarReturned);
        FakeSubscriber notifications = rig.Add("NotificationService", EventTypes.RentalCreated, EventTypes.RentalCancelled);

        await rig.PublishAsync(EventTypes.RentalCreated);
        await rig.PublishAsync(EventTypes.RentalCancelled);
        await rig.PublishAsync(EventTypes.CarReturned);

        Assert.Equal(3, logger.Processed);
        Assert.Equal(2, carService.Processed);
        Assert.Equal(2, notifications.Processed);
        Assert.DoesNotContain(EventTypes.CarReturned, notifications.Received.Select(evt => evt.EventType));
    }

    // Требование 5 Части 2 и 4 Части 3.
    [Fact]
    public async Task Journal_Shows_Routing_And_Delivery_Result()
    {
        var rig = new PubSubRig();
        rig.Add("Logger", EventTypes.RentalCreated);

        await rig.PublishAsync(EventTypes.RentalCreated);

        List<string> lines = rig.Journal.Recent(30);

        Assert.Contains(lines, line => line.Contains("EVENT RECEIVED RentalCreated"));
        Assert.Contains(lines, line => line.Contains("ROUTING RentalCreated -> Logger"));
        Assert.Contains(lines, line => line.Contains("Logger -> SUCCESS"));
    }

    // Требование 3 Части 3.
    [Fact]
    public async Task Broker_Keeps_Working_When_Subscriber_Fails()
    {
        var rig = new PubSubRig();
        FakeSubscriber broken = rig.Add("CarService", EventTypes.RentalCreated);
        FakeSubscriber logger = rig.Add("Logger", EventTypes.RentalCreated);

        broken.Available = false;
        PublishResultMessage first = await rig.PublishAsync(EventTypes.RentalCreated);

        Assert.Equal(1, first.DeliveredCount);
        Assert.Equal(1, first.FailedCount);
        Assert.Equal(1, logger.Processed);
        Assert.Contains("UNAVAILABLE", string.Join(" ", rig.Journal.Recent(30)));

        broken.Available = true;
        PublishResultMessage second = await rig.PublishAsync(EventTypes.RentalCreated, entityId: 2);

        Assert.Equal(2, second.DeliveredCount);
        Assert.Equal(0, second.FailedCount);
    }

    [Fact]
    public async Task Several_Unavailable_Subscribers_Do_Not_Stop_Broker()
    {
        var rig = new PubSubRig();
        FakeSubscriber down1 = rig.Add("CarService", EventTypes.RentalCreated);
        FakeSubscriber down2 = rig.Add("NotificationService", EventTypes.RentalCreated);
        FakeSubscriber logger = rig.Add("Logger", EventTypes.RentalCreated);

        down1.Available = false;
        down2.Available = false;

        PublishResultMessage result = await rig.PublishAsync(EventTypes.RentalCreated);

        Assert.Equal(2, result.FailedCount);
        Assert.Equal(1, logger.Processed);

        BrokerInfoResultMessage info = rig.Broker.GetInfo();
        Assert.True(info.Success);
        Assert.Equal(3, info.OnlineSubscribers);
    }

    // Требование 5 Части 4: CONNECTION_LOST — не авария Broker.
    [Fact]
    public async Task Lost_Subscriber_Connection_Is_Recorded()
    {
        var rig = new PubSubRig();
        FakeSubscriber logger = rig.Add("Logger", EventTypes.RentalCreated);
        logger.DropsConnection = true;

        PublishResultMessage result = await rig.PublishAsync(EventTypes.RentalCreated);

        Assert.Equal(1, result.FailedCount);
        Assert.Contains("CONNECTION_LOST", string.Join(" ", rig.Journal.Recent(30)));
    }

    // Требование 1 Части 4.
    [Fact]
    public async Task New_Subscriber_Added_Later_Receives_Next_Event()
    {
        var rig = new PubSubRig();
        FakeSubscriber logger = rig.Add("Logger", EventTypes.RentalCreated);

        await rig.PublishAsync(EventTypes.RentalCreated);
        Assert.Equal(1, logger.Processed);

        FakeSubscriber audit = rig.Add("AuditService", EventTypes.RentalCreated);
        await rig.PublishAsync(EventTypes.RentalCreated, entityId: 3);

        Assert.Equal(2, logger.Processed);
        Assert.Single(audit.Received);
        Assert.Equal(3, audit.Received[0].EntityId);
    }

    // Требование 1 Части 4.
    [Fact]
    public async Task Same_Publish_Call_Reaches_Old_And_New_Subscriber()
    {
        var rig = new PubSubRig();
        FakeSubscriber logger = rig.Add("Logger", EventTypes.RentalCreated);

        DomainEvent evt = rig.Event(EventTypes.RentalCreated, entityId: 10);
        await rig.Broker.PublishAsync(evt, awaitDelivery: true);

        FakeSubscriber carService = rig.Add("CarService", EventTypes.RentalCreated);

        // Тот же вызов, тот же код публикации: изменился только реестр подписок Broker'а.
        await rig.Broker.PublishAsync(evt, awaitDelivery: true);

        Assert.Equal(2, logger.Processed);
        Assert.Equal(1, carService.Processed);
    }

    // Требование 2 Части 2.
    [Fact]
    public async Task Unsubscribe_Stops_Delivery()
    {
        var rig = new PubSubRig();
        FakeSubscriber logger = rig.Add("Logger", EventTypes.RentalCreated);

        rig.Broker.Subscriptions.UnsubscribeAll("Logger");
        PublishResultMessage result = await rig.PublishAsync(EventTypes.RentalCreated);

        Assert.Equal("NO_SUBSCRIBERS", result.Status);
        Assert.Equal(0, logger.Processed);
    }

    // Требование 5 Части 4: нет подписки на тип.
    [Fact]
    public async Task Event_Without_Subscribers_Is_Reported_Not_Crashing()
    {
        var rig = new PubSubRig();
        rig.Add("Logger", EventTypes.CarReturned);

        PublishResultMessage result = await rig.PublishAsync(EventTypes.RentalCreated);

        Assert.True(result.Success);
        Assert.Equal("NO_SUBSCRIBERS", result.Status);
        Assert.Contains("NO SUBSCRIBERS", string.Join(" ", rig.Journal.Recent(10)));
    }

    [Fact]
    public async Task Unknown_Event_Type_Is_Rejected()
    {
        var rig = new PubSubRig();
        FakeSubscriber logger = rig.Add("Logger", EventTypes.RentalCreated);

        PublishResultMessage result = await rig.PublishAsync("PaymentCompleted");

        Assert.False(result.Success);
        Assert.Equal("UNKNOWN_EVENT_TYPE", result.Status);
        Assert.Equal(0, logger.Processed);
    }

    [Fact]
    public async Task Invalid_Event_Is_Rejected()
    {
        var rig = new PubSubRig();
        rig.Add("Logger", EventTypes.RentalCreated);

        PublishResultMessage noType = await rig.Broker.PublishAsync(
            new DomainEvent { EventType = string.Empty, EntityId = 1 }, awaitDelivery: true);

        PublishResultMessage noEntity = await rig.Broker.PublishAsync(
            new DomainEvent { EventType = EventTypes.RentalCreated, EntityId = 0 }, awaitDelivery: true);

        Assert.Equal("INVALID_EVENT", noType.Status);
        Assert.Equal("INVALID_EVENT", noEntity.Status);
        Assert.Contains("INVALID EVENT", string.Join(" ", rig.Journal.Recent(10)));
    }

    [Fact]
    public void Subscribe_For_Unknown_Subscriber_Fails()
    {
        var rig = new PubSubRig();

        SubscriptionResult result = rig.Broker.Subscriptions.Subscribe("Ghost", new[] { EventTypes.RentalCreated });

        Assert.False(result.Success);
        Assert.Equal("UNKNOWN_SUBSCRIBER", result.Status);
    }

    [Fact]
    public void Subscribe_Without_Event_Type_Fails()
    {
        var rig = new PubSubRig();
        rig.Add("Logger");

        SubscriptionResult result = rig.Broker.Subscriptions.Subscribe("Logger", Array.Empty<string>());

        Assert.False(result.Success);
        Assert.Equal("NO_EVENT_TYPES", result.Status);
    }

    // Требование 3 Части 4.
    [Fact]
    public async Task Delivery_Retries_Until_Success()
    {
        var rig = new PubSubRig();
        FakeSubscriber logger = rig.Add("Logger", EventTypes.RentalCreated);
        logger.FailTimes(2);

        PublishResultMessage result = await rig.PublishAsync(EventTypes.RentalCreated);

        Assert.Equal(1, result.DeliveredCount);
        Assert.Equal(1, logger.Processed);
        Assert.Equal(2, logger.Failed);
        Assert.Contains("attempt 3", string.Join(" ", rig.Journal.Recent(20)));
    }

    // Требование 2 Части 5.
    [Fact]
    public async Task Queue_Keeps_Event_While_Subscriber_Down_And_Delivers_After_Recovery()
    {
        var rig = new PubSubRig();
        FakeSubscriber carService = rig.Add("CarService", EventTypes.RentalCreated);
        carService.Available = false;

        await rig.PublishAsync(EventTypes.RentalCreated, entityId: 5);

        Assert.Equal(0, carService.Processed);
        Assert.Equal(1, rig.Broker.Queue.Count("CarService"));
        Assert.Contains("QUEUED", string.Join(" ", rig.Journal.Recent(20)));

        carService.Available = true;
        int delivered = await rig.Broker.FlushQueueAsync(carService.Channel);

        Assert.Equal(1, delivered);
        Assert.Equal(0, rig.Broker.Queue.Count("CarService"));
        Assert.Single(carService.Received);
        Assert.Equal(5, carService.Received[0].EntityId);
    }

    // Требование 1 Части 5: без очереди событие теряется сознательно.
    [Fact]
    public async Task Subscriber_Without_Queue_Loses_Event_After_Failure()
    {
        var rig = new PubSubRig();
        FakeSubscriber logger = rig.Add("Logger", EventTypes.RentalCreated);
        logger.Channel.Reconnectable = false;
        logger.Available = false;

        await rig.PublishAsync(EventTypes.RentalCreated);

        Assert.Equal(0, rig.Broker.Queue.Count("Logger"));
        Assert.Contains("DROPPED", string.Join(" ", rig.Journal.Recent(20)));
    }

    // Требование 2 Части 5.
    [Fact]
    public async Task Queue_Overflow_Is_Logged_And_Broker_Survives()
    {
        var rig = new PubSubRig();
        FakeSubscriber carService = rig.Add("CarService", EventTypes.RentalCreated);
        carService.Available = false;

        for (int i = 1; i <= EventQueue.PerSubscriberLimit + 3; i++)
        {
            await rig.PublishAsync(EventTypes.RentalCreated, entityId: i);
        }

        Assert.Equal(EventQueue.PerSubscriberLimit, rig.Broker.Queue.Count("CarService"));
        Assert.Contains("QUEUE FULL", string.Join(" ", rig.Journal.Recent(EventQueue.PerSubscriberLimit * 4)));

        carService.Available = true;
        int delivered = await rig.Broker.FlushQueueAsync(carService.Channel);

        Assert.Equal(EventQueue.PerSubscriberLimit, delivered);
    }

    // Требование 2 Части 5: повторы из очереди ограничены — иначе вечно недоступный подписчик зациклит очередь.
    [Fact]
    public async Task Queue_Redelivery_Is_Limited_And_Event_Becomes_Dead_Letter()
    {
        var rig = new PubSubRig();
        FakeSubscriber carService = rig.Add("CarService", EventTypes.RentalCreated);
        carService.Available = false;

        await rig.PublishAsync(EventTypes.RentalCreated, entityId: 9);

        Assert.Equal(1, rig.Broker.Queue.Count("CarService"));

        for (int i = 0; i <= EventQueue.MaxRedeliveries; i++)
        {
            await rig.Broker.FlushQueueAsync(carService.Channel);
        }

        Assert.Equal(0, rig.Broker.Queue.Count("CarService"));
        Assert.Equal(0, carService.Processed);
        Assert.Contains("DEAD LETTER", string.Join(" ", rig.Journal.Recent(40)));
    }

    // Ошибка «подписчик молчит» — Broker фиксирует TIMEOUT по таймауту доставки.
    [Fact]
    public async Task Silent_Subscriber_Times_Out()
    {
        var journal = new EventJournal();
        var broker = new EventBroker("Broker-timeout", journal, retryDelayMs: 0, deliveryTimeoutMs: 100);
        var channel = new SubscriberChannel("Slow") { Online = true, Reconnectable = false };
        channel.Send = async (_, token) =>
        {
            await Task.Delay(2000, token);
            return new DeliveryResultMessage { Success = true, Status = "OK" };
        };
        channel.EventTypes.Add(EventTypes.RentalCreated);
        broker.Subscriptions.Add(channel);

        PublishResultMessage result = await broker.PublishAsync(
            new DomainEvent { EventType = EventTypes.RentalCreated, EntityId = 1 }, awaitDelivery: true);

        Assert.Equal(1, result.FailedCount);
        Assert.Contains("TIMEOUT", string.Join(" ", journal.Recent(20)));
    }

    // Требование 1 Части 5.
    [Fact]
    public async Task Two_Brokers_Deliver_Same_Event_Independently()
    {
        var first = new PubSubRig();
        var second = new PubSubRig();
        FakeSubscriber loggerFirst = first.Add("Logger", EventTypes.RentalCreated);
        FakeSubscriber loggerSecond = second.Add("Logger", EventTypes.RentalCreated);
        loggerSecond.Available = false;

        var evt = new DomainEvent { EventType = EventTypes.RentalCreated, EntityId = 9 };

        PublishResultMessage resultFirst = await first.Broker.PublishAsync(evt, awaitDelivery: true);
        PublishResultMessage resultSecond = await second.Broker.PublishAsync(evt, awaitDelivery: true);

        Assert.Equal("ACCEPTED", resultFirst.Status);
        Assert.Equal(1, loggerFirst.Processed);
        Assert.Equal(1, resultSecond.FailedCount);
        Assert.Equal(0, loggerSecond.Processed);
    }

    // Требование 4 Части 1 и Часть 5.
    [Fact]
    public void Broker_Info_Shows_Subscriptions_And_Queue()
    {
        var rig = new PubSubRig();
        rig.Add("Logger", EventTypes.RentalCreated, EventTypes.CarReturned);
        rig.Add("CarService", EventTypes.RentalCreated);

        BrokerInfoResultMessage info = rig.Broker.GetInfo();

        Assert.True(info.Success);
        Assert.Equal("READY", info.Status);
        Assert.Equal(2, info.OnlineSubscribers);
        Assert.Contains(info.Subscriptions, line => line.StartsWith("Logger:") && line.Contains("CarReturned"));
        Assert.Contains(info.Statuses, line => line.Contains("CarService: ONLINE"));
    }
}
