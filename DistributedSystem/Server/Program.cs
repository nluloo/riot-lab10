using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Npgsql;
using Server;
using Shared.Protocol;

Console.OutputEncoding = Encoding.UTF8;

// Значения по умолчанию совпадают с константами ЛР №7, поэтому запуск на хосте не меняется.
int port = ReadIntEnvironment("SERVER_PORT", 5000);
int processingDelayMs = ReadIntEnvironment("PROCESSING_DELAY_MS", 1500);
int heartbeatIntervalMs = ReadIntEnvironment("HEARTBEAT_INTERVAL_MS", 2000);
string appMode = Environment.GetEnvironmentVariable("APP_MODE") ?? "standalone";

string primaryConnection = Environment.GetEnvironmentVariable("CARRENTAL_PRIMARY_CONNECTION")
    ?? CarRentalDatabase.DefaultPrimaryConnectionString;

string replicaConnection = Environment.GetEnvironmentVariable("CARRENTAL_REPLICA_CONNECTION")
    ?? CarRentalDatabase.DefaultReplicaConnectionString;

var journal = new Journal();

// Пустая строка — сервер работает без Pub/Sub, ровно как в ЛР №5.
string brokersSetting = Environment.GetEnvironmentVariable("PUBLISHER_BROKERS") ?? "127.0.0.1:5001";
Publisher? publisher = string.IsNullOrWhiteSpace(brokersSetting) ? null : new Publisher(brokersSetting, journal);

// Пустая строка — сервер работает без кластера узлов, ровно как в ЛР №6.
string clusterSetting = Environment.GetEnvironmentVariable("CLUSTER_MEMBERS")
    ?? "1=127.0.0.1:6001,2=127.0.0.1:6002,3=127.0.0.1:6003,4=127.0.0.1:6004,5=127.0.0.1:6005";

Cluster? cluster = null;

try
{
    cluster = string.IsNullOrWhiteSpace(clusterSetting) ? null : new Cluster(clusterSetting, journal);
}
catch (FormatException ex)
{
    Console.WriteLine($"Кластер узлов выключен: {ex.Message}");
}

var primary = new DataInstance("Primary", new CarRentalDatabase("Primary", primaryConnection));
var replica = new DataInstance("Replica", new CarRentalDatabase("Replica", replicaConnection));
var instances = new InstanceManager(primary, replica, journal);
var replicator = new Replicator(journal);
var requestHandler = new RequestHandler(instances, replicator, journal, processingDelayMs, publisher, cluster);

Console.WriteLine("Сервер аренды автомобилей (ЛР №9, Kubernetes)");
Console.WriteLine($"Application mode (APP_MODE): {appMode}");
journal.Write("Server", $"Режим работы (APP_MODE): {appMode}");
Console.WriteLine("Архитектура: Client -> Server -> Primary + Replica (два экземпляра PostgreSQL)");
Console.WriteLine("              Server (Publisher) -> Broker -> Subscriber");
Console.WriteLine("              Server -> Cluster (5 узлов: выборы лидера + консенсус по большинству)");
Console.WriteLine($"Порт: {port}. Задержка до lock: {processingDelayMs} мс. Проверка состояния: каждые {heartbeatIntervalMs} мс");
Console.WriteLine("Общий ресурс: свободный автомобиль (cars.is_available), реплицируются locations, cars, customers, rentals");
Console.WriteLine("Репликация: изменение в активной копии -> передача затронутых таблиц во вторую копию");
Console.WriteLine("Отказоустойчивость: отказ Primary -> Failover на Replica -> восстановление -> синхронизация");
Console.WriteLine(publisher == null
    ? "Pub/Sub: ВЫКЛЮЧЕН (PUBLISHER_BROKERS пуст) — операции работают как в ЛР №5"
    : $"Pub/Sub: Publisher публикует события в {string.Join(", ", publisher.Endpoints.Select(endpoint => endpoint.Label))}");
Console.WriteLine(cluster == null || !cluster.Enabled
    ? "Кластер узлов: ВЫКЛЮЧЕН (CLUSTER_MEMBERS пуст) — операции работают как в ЛР №6"
    : $"Кластер узлов: {cluster.TotalNodes} узлов ({string.Join(", ", cluster.Members.Select(member => member.Endpoint))}), большинство {cluster.Majority}");

PrepareData(primary, replica, instances, replicator, journal);

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
    journal.Write("Server", "STOPPING, получен SIGTERM (docker stop)");
    cts.Cancel();
});

_ = Task.Run(() => instances.MonitorAsync(heartbeatIntervalMs, cts.Token));

// IPAddress.Any: внутри контейнера 127.0.0.1 доступен только самому контейнеру,
// клиент и Broker подключаются к серверу снаружи.
var listener = new TcpListener(IPAddress.Any, port);
int clientCounter = 0;

listener.Start();
journal.Write("Server", "READY, ожидание клиентов...");

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

    int clientId = Interlocked.Increment(ref clientCounter);
    _ = ProcessClientAsync(client, requestHandler, clientId, journal);
}

listener.Stop();
Console.WriteLine("Сервер остановлен.");

static void PrepareData(
    DataInstance primary,
    DataInstance replica,
    InstanceManager instances,
    Replicator replicator,
    Journal journal)
{
    instances.RefreshStates();
    journal.Write("Startup", $"Primary -> {primary.StatusText}, Replica -> {replica.StatusText}");

    foreach (DataInstance instance in new[] { primary, replica })
    {
        if (!instance.IsAvailable)
        {
            journal.Write(instance.Name, "UNAVAILABLE, сервер продолжит работу без этого экземпляра");
            continue;
        }

        try
        {
            instance.Database.EnsureCreated();
            journal.Write(instance.Name, $"SCHEMA READY, lsn = {instance.Database.GetLsn()}");
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
        {
            journal.Write(instance.Name, $"Ошибка подготовки схемы: {ex.Message}");
        }
    }

    if (!primary.IsAvailable || !replica.IsAvailable)
    {
        journal.Write("Initial", "Проверка начального состояния пропущена: один из экземпляров недоступен");
        return;
    }

    try
    {
        DiffReport report = replicator.Compare(primary, replica);

        if (report.IsConsistent)
        {
            journal.Write("Initial", $"Primary = Replica (lsn = {report.PrimaryLsn}) — начальное состояние совпадает");
        }
        else
        {
            journal.Write("Initial", $"Копии различаются ({report.Differences.Count} отличий), выполняется первичная синхронизация");
            replicator.FullSync(primary, replica, "Initial");
        }
    }
    catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
    {
        journal.Write("Initial", $"Не удалось проверить начальное состояние: {ex.Message}");
    }
}

static async Task ProcessClientAsync(
    TcpClient client,
    RequestHandler requestHandler,
    int clientId,
    Journal journal)
{
    using (client)
    {
        NetworkStream stream = client.GetStream();

        try
        {
            using var reader = new StreamReader(stream);
            using var writer = new StreamWriter(stream) { AutoFlush = true };

            string? requestJson;

            try
            {
                requestJson = await reader.ReadLineAsync();
            }
            catch (IOException)
            {
                journal.Write($"Client {clientId}", "клиент отключился до отправки запроса");
                return;
            }

            if (string.IsNullOrWhiteSpace(requestJson))
            {
                await SendResponseAsync(writer, new Response { Success = false, Message = "Получен пустой запрос" }, clientId);
                return;
            }

            Request? request;

            try
            {
                request = JsonSerializer.Deserialize<Request>(requestJson,
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            }
            catch (JsonException)
            {
                await SendResponseAsync(writer, new Response { Success = false, Message = "Некорректный JSON" }, clientId);
                return;
            }

            if (request == null || string.IsNullOrWhiteSpace(request.Operation))
            {
                await SendResponseAsync(writer, new Response { Success = false, Message = "Некорректный запрос" }, clientId);
                return;
            }

            Response response;

            try
            {
                response = await requestHandler.HandleAsync(request, clientId);
            }
            catch (Exception ex)
            {
                journal.Write($"Client {clientId}", $"ошибка при обработке запроса: {ex.Message}");
                response = new Response { Success = false, Message = "Внутренняя ошибка сервера при обработке запроса" };
            }

            await SendResponseAsync(writer, response, clientId);
        }
        catch (IOException)
        {
            journal.Write($"Client {clientId}", "ошибка сетевого взаимодействия (клиент отключился)");
        }
        catch (SocketException)
        {
            journal.Write($"Client {clientId}", "соединение прервано (SocketException)");
        }
    }
}

static async Task SendResponseAsync(StreamWriter writer, Response response, int clientId)
{
    try
    {
        string responseJson = JsonSerializer.Serialize(response,
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

        await writer.WriteLineAsync(responseJson);
    }
    catch (IOException)
    {
        Console.WriteLine($"Client {clientId}: не удалось отправить ответ — клиент уже отключился");
    }
}

static int ReadIntEnvironment(string name, int defaultValue)
{
    string? raw = Environment.GetEnvironmentVariable(name);

    return int.TryParse(raw, out int value) && value > 0 ? value : defaultValue;
}
