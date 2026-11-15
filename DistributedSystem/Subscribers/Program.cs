using System.Text;
using Shared.Events;
using Subscribers;

Console.OutputEncoding = Encoding.UTF8;

string brokerHost = Environment.GetEnvironmentVariable("SUBSCRIBER_BROKER_HOST") ?? "127.0.0.1";
int brokerPort = ReadIntEnvironment("SUBSCRIBER_BROKER_PORT", 5001);

var journal = new EventJournal();
var subscribers = new List<SubscriberInstance>();

// Вариант 11 «Аренда автомобилей». Задержки 1/3/2 с — из Части 3 методички.
// Logger очередь не копит (что не доставлено — потеряно), CarService и NotificationService — копят.
subscribers.Add(new SubscriberInstance(
    "Logger", 1000,
    new[] { EventTypes.RentalCreated, EventTypes.RentalCancelled, EventTypes.CarReturned },
    reconnectable: false, journal));

subscribers.Add(new SubscriberInstance(
    "CarService", 3000,
    new[] { EventTypes.RentalCreated, EventTypes.CarReturned },
    reconnectable: true, journal));

subscribers.Add(new SubscriberInstance(
    "NotificationService", 2000,
    new[] { EventTypes.RentalCreated, EventTypes.RentalCancelled },
    reconnectable: true, journal));

// Часть 5, Требование 4: аналитика собирает только счётчики (Processed, CountsByType),
// данные предметной области не трогает и кода Publisher/Broker не требует.
subscribers.Add(new SubscriberInstance(
    "AnalyticsService", 500,
    new[] { EventTypes.RentalCreated, EventTypes.RentalCancelled, EventTypes.CarReturned },
    reconnectable: true, journal));

Console.WriteLine("Подписчики — ЛР №6, модель Pub/Sub");
Console.WriteLine($"Broker: {brokerHost}:{brokerPort} (переменные SUBSCRIBER_BROKER_HOST и SUBSCRIBER_BROKER_PORT)");

foreach (SubscriberInstance subscriber in subscribers)
{
    await ConnectAsync(subscriber);
}

while (true)
{
    Console.WriteLine("\nПодписчики (ЛР №6) — Pub/Sub");
    Console.WriteLine("1  - Список подписчиков: состояние, режим, задержка, подписки");
    Console.WriteLine("2  - Подписаться на события");
    Console.WriteLine("3  - Отменить все подписки");
    Console.WriteLine("4  - Сменить режим: AVAILABLE / UNAVAILABLE / FAILING / NO ANSWER");
    Console.WriteLine("5  - Остановить подписчика (закрыть соединение с Broker)");
    Console.WriteLine("6  - Возобновить подписчика (переподключиться и забрать очередь)");
    Console.WriteLine("7  - Добавить нового подписчика (Publisher не меняется)");
    Console.WriteLine("8  - Обработанные события и статистика");
    Console.WriteLine("9  - Состояние Broker: подписки, кто на связи, очередь");
    Console.WriteLine("10 - Сводка аналитики: события по типам и время обработки у каждого подписчика");
    Console.WriteLine("0  - Выход");
    Console.Write("Выберите операцию: ");

    string? choice = Console.ReadLine();

    // null — это EOF или Ctrl+Z: без выхода меню напечатало бы себя бесконечно.
    if (choice == null || choice == "0")
    {
        break;
    }

    switch (choice.Trim())
    {
        case "1":
            ShowSubscribers();
            break;

        case "2":
        {
            SubscriberInstance? subscriber = ChooseSubscriber();

            if (subscriber == null)
            {
                break;
            }

            Console.Write("Типы событий через запятую (RentalCreated, CarReturned): ");
            List<string> types = ParseTypes(ReadLineOrEmpty());
            await subscriber.SubscribeAsync(types);
            break;
        }

        case "3":
        {
            SubscriberInstance? subscriber = ChooseSubscriber();

            if (subscriber != null)
            {
                await subscriber.UnsubscribeAsync();
            }

            break;
        }

        case "4":
        {
            SubscriberInstance? subscriber = ChooseSubscriber();

            if (subscriber == null)
            {
                break;
            }

            Console.WriteLine("1 - AVAILABLE, 2 - UNAVAILABLE, 3 - FAILING TWICE, 4 - NO ANSWER");
            Console.Write("Режим: ");

            switch (ReadLineOrEmpty().Trim())
            {
                case "1":
                    subscriber.Mode = SubscriberMode.Available;
                    break;
                case "2":
                    subscriber.Mode = SubscriberMode.Unavailable;
                    break;
                case "3":
                    subscriber.Mode = SubscriberMode.FailingTwice;
                    break;
                case "4":
                    subscriber.Mode = SubscriberMode.NoAnswer;
                    break;
                default:
                    journal.Write(subscriber.Name, "режим не изменён: такой цифры в списке нет");
                    break;
            }

            journal.Write(subscriber.Name, $"MODE -> {subscriber.ModeText}");
            break;
        }

        case "5":
        {
            SubscriberInstance? subscriber = ChooseSubscriber();

            if (subscriber != null)
            {
                await subscriber.DisconnectAsync("остановлен из меню — так моделируется отказ подписчика");
            }

            break;
        }

        case "6":
        {
            SubscriberInstance? subscriber = ChooseSubscriber();

            if (subscriber != null)
            {
                await ConnectAsync(subscriber);
            }

            break;
        }

        case "7":
        {
            Console.Write("Имя нового подписчика (например, AuditService): ");
            string name = ReadLineOrEmpty().Trim();

            if (name.Length == 0)
            {
                Console.WriteLine("Имя не введено — подписчик не добавлен.");
                break;
            }

            if (subscribers.Any(subscriber => subscriber.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
            {
                Console.WriteLine($"Подписчик '{name}' уже есть — новое имя отличимо от существующих.");
                break;
            }

            Console.Write("Задержка обработки, мс (1000): ");
            int delay = ReadIntLine(1000);

            Console.Write("Типы событий через запятую (RentalCreated): ");
            List<string> types = ParseTypes(ReadLineOrEmpty());

            var subscriber = new SubscriberInstance(name, delay, types, reconnectable: true, journal);
            subscribers.Add(subscriber);
            await ConnectAsync(subscriber);
            Console.WriteLine($"Добавлен '{name}'. Publisher не изменился: он по-прежнему просто публикует события.");
            break;
        }

        case "8":
        {
            SubscriberInstance? subscriber = ChooseSubscriber();

            if (subscriber == null)
            {
                break;
            }

            Console.WriteLine($"{subscriber.Name}: обработано {subscriber.Processed}, отклонено {subscriber.Failed}, последний раз {subscriber.LastProcessedMs} мс");

            foreach ((string type, int count) in subscriber.CountsByType.OrderBy(pair => pair.Key))
            {
                Console.WriteLine($"  {type}: {count}");
            }

            foreach (string line in subscriber.ReceivedLines.Take(10))
            {
                Console.WriteLine($"  получил: {line}");
            }

            break;
        }

        case "9":
        {
            SubscriberInstance? connected = subscribers.FirstOrDefault(subscriber => subscriber.IsConnected);

            if (connected == null)
            {
                Console.WriteLine("Ни один подписчик не на связи — запросить состояние Broker не через кого.");
                break;
            }

            BrokerInfoResultMessage? info = await connected.GetBrokerInfoAsync();

            if (info == null)
            {
                Console.WriteLine("Broker не ответил: соединение недоступно.");
                break;
            }

            Console.WriteLine($"Broker {info.BrokerId}: подписчиков на связи {info.OnlineSubscribers}, событий в очереди {info.QueuedEvents}");

            foreach (string line in info.Subscriptions)
            {
                Console.WriteLine($"  подписка: {line}");
            }

            foreach (string line in info.Statuses)
            {
                Console.WriteLine($"  состояние: {line}");
            }

            break;
        }

        case "10":
        {
            var totals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            foreach (SubscriberInstance subscriber in subscribers)
            {
                foreach ((string type, int count) in subscriber.CountsByType)
                {
                    totals.TryGetValue(type, out int known);
                    totals[type] = known + count;
                }
            }

            Console.WriteLine($"Всего событий обработано: {subscribers.Sum(subscriber => subscriber.Processed)} " +
                              $"(отказов {subscribers.Sum(subscriber => subscriber.Failed)})");

            foreach ((string type, int count) in totals.OrderBy(pair => pair.Key))
            {
                Console.WriteLine($"  {type}: {count}");
            }

            foreach (SubscriberInstance subscriber in subscribers)
            {
                Console.WriteLine($"  {subscriber.Name,-20} обработано {subscriber.Processed}, последняя обработка {subscriber.LastProcessedMs} мс");
            }

            break;
        }

        default:
            Console.WriteLine("Такого пункта нет — выберите число из списка.");
            break;
    }
}

foreach (SubscriberInstance subscriber in subscribers)
{
    subscriber.Dispose();
}

Console.WriteLine("Подписчики остановлены.");

async Task ConnectAsync(SubscriberInstance subscriber)
{
    try
    {
        await subscriber.ConnectAsync(brokerHost, brokerPort);
    }
    catch (PubSubTransportException ex)
    {
        journal.Write(subscriber.Name, $"BROKER UNAVAILABLE: {ex.Message} — подписчик не подключён, остальные работают");
    }
}

void ShowSubscribers()
{
    foreach (SubscriberInstance subscriber in subscribers)
    {
        Console.WriteLine($"{subscriber.Name,-20} {(subscriber.IsConnected ? "CONNECTED" : "OFFLINE"),-9} {subscriber.ModeText,-16} " +
                          $"{subscriber.DelayMs} мс, подписки: {(subscriber.Subscriptions.Count > 0 ? string.Join(", ", subscriber.Subscriptions) : "нет")}, " +
                          $"обработано {subscriber.Processed}");
    }
}

SubscriberInstance? ChooseSubscriber()
{
    ShowSubscribers();
    Console.Write("Имя подписчика: ");
    string name = ReadLineOrEmpty().Trim();

    SubscriberInstance? subscriber = subscribers.FirstOrDefault(item => item.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

    if (subscriber == null)
    {
        Console.WriteLine($"Подписчик не зарегистрирован: '{name}'. Такого нет в списке.");
    }

    return subscriber;
}

static List<string> ParseTypes(string raw) => raw
    .Split(new[] { ',', ';', ' ' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
    .ToList();

static string ReadLineOrEmpty() => Console.ReadLine() ?? string.Empty;

static int ReadIntLine(int defaultValue)
{
    string raw = ReadLineOrEmpty().Trim();

    return int.TryParse(raw, out int value) && value >= 0 ? value : defaultValue;
}

static int ReadIntEnvironment(string name, int defaultValue)
{
    string? raw = Environment.GetEnvironmentVariable(name);

    return int.TryParse(raw, out int value) && value > 0 ? value : defaultValue;
}
