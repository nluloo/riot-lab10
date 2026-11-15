using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Broker;
using Shared.Events;

Console.OutputEncoding = Encoding.UTF8;

string brokerId = Environment.GetEnvironmentVariable("BROKER_ID") ?? "Broker-1";
int port = ReadPort("BROKER_PORT", 5001);
int retryDelayMs = ReadInt("BROKER_RETRY_DELAY_MS", 1500);
int queuePumpMs = ReadInt("BROKER_QUEUE_PUMP_MS", 5000);

var journal = new EventJournal();
var broker = new EventBroker(brokerId, journal, retryDelayMs);

Console.WriteLine($"Broker {brokerId} — ЛР №6, модель Pub/Sub");
Console.WriteLine($"Порт: {port}. Повторная доставка: не более {DeliveryManager.MaxAttempts} попыток, пауза {retryDelayMs} мс.");
Console.WriteLine($"Очередь на подписчика: {EventQueue.PerSubscriberLimit} событий, не более {EventQueue.MaxRedeliveries} повторов, проверка очереди каждые {queuePumpMs} мс.");
Console.WriteLine("Publisher -> Broker -> Subscriber. Broker знает подписки, Publisher — нет.");

var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

// docker stop присылает SIGTERM, а CancelKeyPress ловит только Ctrl+C: без этой регистрации
// контейнер умирает без последней строки журнала. Cancel = true — цикл сам завершится.
using PosixSignalRegistration sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, context =>
{
    context.Cancel = true;
    journal.Write(brokerId, "STOPPING, получен SIGTERM (docker stop)");
    cts.Cancel();
});

_ = Task.Run(() => PumpQueuesAsync(broker, journal, queuePumpMs, cts.Token));

// IPAddress.Any: внутри контейнера 127.0.0.1 доступен только самому контейнеру,
// Publisher (Server) и подписчики подключаются к брокеру из других контейнеров.
var listener = new TcpListener(IPAddress.Any, port);

listener.Start();
journal.Write(brokerId, "READY, ожидание Publisher и подписчиков...");

while (!cts.Token.IsCancellationRequested)
{
    TcpClient client;

    try
    {
        client = await listener.AcceptTcpClientAsync(cts.Token);
    }
    catch (OperationCanceledException)
    {
        break;
    }

    _ = Task.Run(() => ServeConnectionAsync(client, broker, journal, cts.Token), cts.Token);
}

listener.Stop();
Console.WriteLine($"Broker {brokerId} остановлен.");

// Раз в queuePumpMs проверяем: подписчик снова на связи и для него что-то лежит — доставляем.
static async Task PumpQueuesAsync(EventBroker broker, EventJournal journal, int intervalMs, CancellationToken cancellationToken)
{
    while (!cancellationToken.IsCancellationRequested)
    {
        try
        {
            await Task.Delay(intervalMs, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        foreach (string name in broker.Queue.Names())
        {
            SubscriberChannel? channel = broker.Subscriptions.Find(name);

            if (channel == null || !channel.Online || broker.Queue.Count(name) == 0)
            {
                continue;
            }

            try
            {
                await broker.FlushQueueAsync(channel);
            }
            catch (Exception ex) when (ex is PubSubTransportException or IOException)
            {
                journal.Write(broker.BrokerId, $"повторная доставка для {name} не удалась: {ex.Message}");
            }
        }
    }
}

// Роль соединения определяет первое сообщение: Publisher или подписчик. Разных протоколов здесь нет.
static async Task ServeConnectionAsync(TcpClient client, EventBroker broker, EventJournal journal, CancellationToken cancellationToken)
{
    using (client)
    {
        LineConnection connection = LineConnection.FromAccepted(client);

        try
        {
            string? first;

            try
            {
                first = await connection.ReadLineAsync(cancellationToken);
            }
            catch (IOException)
            {
                return;
            }

            if (first == null)
            {
                journal.Write(broker.BrokerId, "соединение закрыто до приветствия");
                return;
            }

            PubSubMessage? hello;

            try
            {
                hello = PubSubJson.Deserialize<PubSubMessage>(first);
            }
            catch (JsonException)
            {
                journal.Write(broker.BrokerId, "НЕКОРРЕКТНЫЙ JSON в приветствии — соединение закрыто");
                return;
            }

            HelloMessage? data = hello == null ? null : PubSubJson.Payload<HelloMessage>(hello);

            if (data == null || string.IsNullOrWhiteSpace(data.Name))
            {
                journal.Write(broker.BrokerId, "HELLO без имени — соединение закрыто");
                return;
            }

            if (hello!.Operation != PubSubOperations.Hello)
            {
                journal.Write(broker.BrokerId, $"первым пришло '{hello.Operation}' вместо hello — соединение закрыто");
                return;
            }

            if (data.Kind.Equals("publisher", StringComparison.OrdinalIgnoreCase))
            {
                await new PublisherConnection(connection, broker, journal, data.Name).RunAsync(cancellationToken);
            }
            else
            {
                await new SubscriberConnection(connection, broker, journal)
                    .RunAsync(data.Name, data.Reconnectable, cancellationToken);
            }
        }
        catch (IOException)
        {
            journal.Write(broker.BrokerId, "соединение разорвано (IOException)");
        }
        catch (OperationCanceledException)
        {
            // Broker завершается — соединения закрываются молча.
        }
    }
}

static int ReadInt(string name, int defaultValue)
{
    string? raw = Environment.GetEnvironmentVariable(name);

    return int.TryParse(raw, out int value) && value > 0 ? value : defaultValue;
}

static int ReadPort(string name, int defaultValue) => ReadInt(name, defaultValue);
