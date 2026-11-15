using System.Net.Sockets;
using Server;
using Shared.Cluster;
using Shared.Events;
using Shared.Protocol;
using Subscribers;
using Xunit.Abstractions;

namespace Tests;

/// <summary>
/// Адрес контейнеризованного стенда (labs/lab8/compose.yaml) со стороны хоста: сервер публикуется
/// на 5000, Broker — на 5001. Протокол тот же, что в ЛР №5…ЛР №7: одна строка JSON = один запрос,
/// поэтому соединение берётся из Shared/Events/LineConnection, а не пишется своё.
/// </summary>
internal static class ContainerStand
{
    public const string Host = "127.0.0.1";
    public const int ServerPort = 5000;
    public const int BrokerPort = 5001;

    private const int ConnectTimeoutMs = 1500;

    /// Сервер перед изменением берёт консенсус у узлов и ждёт PROCESSING_DELAY_MS (1500 мс из compose.yaml).
    private const int AnswerTimeoutMs = 30000;

    /// Гейт: тесты против стенда идут только при RUN_CONTAINER_TESTS=1, чтобы `dotnet test` без
    /// Docker оставался зелёным.
    public static bool Enabled => Environment.GetEnvironmentVariable("RUN_CONTAINER_TESTS") == "1";

    public static string SkipReason =>
        "пропущено: стенд не поднят (docker compose up -d) или не задан RUN_CONTAINER_TESTS=1";

    /// Проверка «гейт открыт и сервис отвечает». xUnit 2.4 не умеет снимать тест динамически,
    /// поэтому тест ничего не проверяет и возвращается успешно, а причину пишет в вывод.
    public static bool Ready(ITestOutputHelper output, int port = ServerPort)
    {
        if (!Enabled)
        {
            output.WriteLine(SkipReason);
            return false;
        }

        try
        {
            LineConnection.Connect(Host, port, ConnectTimeoutMs).Dispose();
            return true;
        }
        catch (PubSubTransportException ex)
        {
            output.WriteLine($"{SkipReason} — {Host}:{port} не отвечает ({ex.Message})");
            return false;
        }
    }

    /// Один запрос — одно соединение: сервер читает строку, отвечает и закрывает соединение.
    public static Response Ask(Request request)
    {
        using LineConnection connection = LineConnection.Connect(Host, ServerPort, ConnectTimeoutMs);

        connection.SendSync(request, AnswerTimeoutMs);
        string? line = connection.ReadLineSync(AnswerTimeoutMs);

        if (line == null)
        {
            throw new PubSubTransportException($"сервер {Host}:{ServerPort} закрыл соединение без ответа");
        }

        return PubSubJson.Deserialize<Response>(line)
               ?? throw new InvalidOperationException($"сервер ответил не сообщением: {line}");
    }
}

/// <summary>
/// Интеграционные тесты против стенда из compose.yaml и шесть случаев отказа:
/// 1) пустой запрос и 2) некорректный JSON и 3) неизвестная операция — ContainerizedServer_InvalidRequest;
/// 4) недоступный Broker и 5) недоступные узлы кластера — Server_UnreachableBrokerAndNodes;
/// 6) недоступны оба экземпляра данных и восстановление — Recovery_AfterServiceRestart.
/// Тесты с приставкой ContainerizedServer идут только при RUN_CONTAINER_TESTS=1.
/// Нагрузочный тест против стенда не повторяется здесь — он в LoadTests.Load_ContainerizedServer_MixedOperations.
/// </summary>
public class ContainerIntegrationTests : IDisposable
{
    /// Номер телефона тестового клиента: GetOrCreateCustomer обновляет ФИО по уже существующему
    /// номеру, поэтому берётся номер, которого в демо-данных стенда нет.
    private const string TestPhone = "375297770088";

    private readonly ITestOutputHelper _output;

    /// Аренда, созданная в тесте, и подписчик теста: xUnit вызывает Dispose после каждого теста,
    /// поэтому стенд возвращается в исходное состояние даже если проверка упала на середине.
    /// Без этого за прогоном остаётся занятый автомобиль и очередь событий на Broker'е.
    private int? _rentalToReturn;
    private SubscriberInstance? _subscriber;

    public ContainerIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public void Dispose()
    {
        _subscriber?.Dispose();
        _subscriber = null;

        if (_rentalToReturn == null || !ContainerStand.Enabled)
        {
            _rentalToReturn = null;
            return;
        }

        try
        {
            ContainerStand.Ask(ReturnCarRequest(_rentalToReturn.Value));
        }
        catch (PubSubTransportException)
        {
            // Стенд уже остановлен — возвращать некуда, проверка состояния стенда не часть уборки.
        }

        _rentalToReturn = null;
    }

    // ---------------------------------------------------------------- стенд целиком

    [Fact]
    public void ContainerStand_GateClosesTestsAndFixesSkipMessage()
    {
        if (!ContainerStand.Enabled)
        {
            Assert.Equal("пропущено: стенд не поднят (docker compose up -d) или не задан RUN_CONTAINER_TESTS=1",
                ContainerStand.SkipReason);
            Assert.False(ContainerStand.Ready(_output));

            return;
        }

        Assert.True(ContainerStand.Ready(_output));
    }

    [Fact]
    public void ContainerizedServer_MainOperations_ChangeData()
    {
        if (!ContainerStand.Ready(_output))
        {
            return;
        }

        Response before = ContainerStand.Ask(TestRig.Empty("ShowState"));
        Assert.True(before.Success);
        int rentalsBefore = RentalsOf(before);

        Response search = ContainerStand.Ask(SearchInMinsk());
        Assert.True(search.Success);

        List<string> availableBefore = RegistrationsOf(search);
        Assert.NotEmpty(availableBefore);
        string registration = availableBefore[0];

        Response created = ContainerStand.Ask(
            TestRig.CreateRental("Контейнер Тест", TestPhone, registration, clientName: "Container test"));

        Assert.True(created.Success);
        Assert.Contains("Номер аренды", created.Message);
        int rentalId = created.Data!.Value.GetProperty("id").GetInt32();
        _rentalToReturn = rentalId;

        // Данные изменились: автомобиль ушёл из поиска, аренда появилась в состоянии.
        Assert.DoesNotContain(registration, RegistrationsOf(ContainerStand.Ask(SearchInMinsk())));
        Assert.Equal(rentalsBefore + 1, RentalsOf(ContainerStand.Ask(TestRig.Empty("ShowState"))));

        Response returned = ContainerStand.Ask(ReturnCarRequest(rentalId));

        Assert.True(returned.Success);
        Assert.Equal("Автомобиль возвращён", returned.Message);
        _rentalToReturn = null;

        // Демо-данные стенда возвращаются в исходное состояние: машина снова в поиске.
        Assert.Contains(registration, RegistrationsOf(ContainerStand.Ask(SearchInMinsk())));

        Response secondReturn = ContainerStand.Ask(new Request
        {
            Operation = "CancelRental",
            Data = PubSubJson.ToElement(new { rentalId })
        });

        Assert.False(secondReturn.Success);
        Assert.Equal("Эту аренду нельзя отменить", secondReturn.Message);
    }

    [Fact]
    public async Task ContainerizedServer_BrokerReceivesEvent_AfterCreateRental()
    {
        if (!ContainerStand.Ready(_output, ContainerStand.BrokerPort))
        {
            return;
        }

        // Имя подписчика уникально для запуска: Broker хранит очередь для reconnectable-подписчика
        // по имени, и с фиксированным именем сюда пришли бы события из предыдущих прогонов.
        string subscriberName = $"Container test {DateTime.Now:HHmmss}";
        var subscriber = new SubscriberInstance(
            subscriberName, 0, new[] { EventTypes.RentalCreated }, reconnectable: true, new EventJournal());

        _subscriber = subscriber;
        await subscriber.ConnectAsync(ContainerStand.Host, ContainerStand.BrokerPort);

        Response status = ContainerStand.Ask(TestRig.Empty("BrokerStatus"));
        List<string> brokerLines = LinesOf(status);

        Assert.True(status.Success);
        Assert.Contains(brokerLines, line => line.Contains("Broker-1") && line.Contains("READY"));

        // Подписчик с хоста виден через состояние Broker, которое сервер запрашивает у Broker'а.
        Assert.Contains(brokerLines, line => line.Contains(subscriberName));
        Assert.Contains(brokerLines, line => line.Contains("подписчиков на связи"));

        Response search = ContainerStand.Ask(SearchInMinsk());
        string registration = RegistrationsOf(search)[0];

        Response created = ContainerStand.Ask(
            TestRig.CreateRental("Контейнер Тест", TestPhone, registration, clientName: "Container event"));

        Assert.True(created.Success);
        Assert.NotNull(created.Events);
        Assert.Contains("ACCEPTED", created.Events);
        Assert.Contains("Broker-1", created.Events);

        // Событие дошло до подписчика: сервер — Publisher, доставку делает Broker.
        await WaitUntilAsync(() => subscriber.Processed >= 1);

        Assert.Equal(1, subscriber.Processed);
        Assert.Equal(0, subscriber.Failed);

        _rentalToReturn = created.Data!.Value.GetProperty("id").GetInt32();
    }

    [Fact]
    public void ContainerizedServer_ConsensusWorks_OnFiveNodesWithMajority()
    {
        if (!ContainerStand.Ready(_output))
        {
            return;
        }

        // ClusterCheck запускает полный раунд heartbeat: после него перевыборы точно завершены,
        // и роль лидера видно в обычном ClusterStatus.
        Response check = ContainerStand.Ask(TestRig.Empty("ClusterCheck"));

        Assert.True(check.Success);
        Assert.StartsWith("Лидер Node", check.Message);
        Assert.NotNull(check.Consensus);
        Assert.Contains("большинство 3", check.Consensus);

        Response status = ContainerStand.Ask(TestRig.Empty("ClusterStatus"));

        Assert.True(status.Success);
        Assert.Equal("Узлов 5, большинство 3.", status.Message);

        List<string> views = LinesOf(status);

        // ClusterStatus отдаёт строку на каждый узел плюс итог: лидер, доступность, большинство.
        Assert.Equal(5, views.Count(view => view.Contains("Version =")));
        Assert.Contains(views, view => view.Contains(ClusterStates.Leader));
        Assert.Contains(views, view => view.Contains("Leader: Node"));
        Assert.Contains(views, view => view.Contains("большинство 3"));
        Assert.Contains(views, view => view.Contains("5/5"));

        // Согласование проверяется делом: предложение принимается, только если большинство проголосовало.
        Response proposal = ContainerStand.Ask(new Request
        {
            Operation = "ClusterPropose",
            Data = PubSubJson.ToElement(new { value = "ЛР №8: проверка консенсуса на стенде", version = 0 })
        });

        Assert.True(proposal.Success);
        Assert.Contains("CONSENSUS ACHIEVED", proposal.Message);
        Assert.Contains("Majority = 3", proposal.Consensus);
    }

    [Fact]
    public async Task ContainerizedServer_InvalidRequest_AnswersErrorAndStaysAlive()
    {
        if (!ContainerStand.Ready(_output))
        {
            return;
        }

        Assert.Equal("Некорректный JSON", (await AskRawLineAsync("не json")).Message);
        Assert.Equal("Получен пустой запрос", (await AskRawLineAsync(string.Empty)).Message);
        Assert.Equal("Неизвестная операция", (await AskRawLineAsync("""{"operation":"NoSuchOperation"}""")).Message);
        Assert.Equal("Некорректный запрос", (await AskRawLineAsync("""{"operation":"   "}""")).Message);

        // Соединение после битых строк не умерло: обычный запрос проходит.
        Response alive = ContainerStand.Ask(TestRig.Empty("ShowState"));

        Assert.True(alive.Success);
        Assert.Contains("Автомобилей", alive.Message);
    }

    // ---------------------------------------------------------------- случаи отказа

    [Fact]
    public async Task Server_UnreachableBrokerAndNodes_CompletesRequestWithoutBoth()
    {
        TestRig.ResetPair();

        var rig = new TestRig();

        // Адреса из compose.yaml снаружи сети не работают: ровно так же ведёт себя остановленный
        // контейнер Broker или узла. Проверка идёт на логике приложения, без docker stop из теста.
        var handler = new RequestHandler(
            rig.Instances,
            rig.Replicator,
            rig.Journal,
            processingDelayMs: 0,
            publisher: new Publisher(Compose.Brokers, rig.Journal),
            cluster: new Cluster(Compose.ClusterMembers, rig.Journal));

        Response created = await handler.HandleAsync(
            TestRig.CreateRental("Илья Гордеев", "375291115566", TestRig.ExperimentCar, clientName: "Degraded"),
            clientId: 7);

        Assert.True(created.Success);
        Assert.Contains("BROKER_UNAVAILABLE", created.Events);

        // Узлы молчат — консенсус пропускается, но запрос выполняется как в ЛР №6.
        Assert.Null(created.Consensus);
        Assert.Contains(rig.LogEntries(), line => line.Contains("Consensus -> SKIPPED"));
        Assert.Contains(rig.LogEntries(), line => line.Contains("BROKER UNAVAILABLE"));

        Assert.Equal(1, rig.Primary.Database.CountRentals());
        Assert.True(rig.Replicator.Compare(rig.Primary, rig.Replica).IsConsistent);
    }

    [Fact]
    public async Task Recovery_AfterServiceRestart_NewInstanceReadsRecordsOfTheOldOne()
    {
        TestRig.ResetPair();

        // «Остановленный primary» моделируется строкой подключения к свободному порту — тот же
        // механизм, что у docker stop: Ping() не проходит, экземпляр помечен UNAVAILABLE.
        var rig = new TestRig(primary: TestRig.UnavailableDatabase);

        Response created = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Сергей Смирнов", "375291112233", TestRig.ExperimentCar, clientName: "Restart"),
            clientId: 3);

        Assert.True(created.Success);
        Assert.Equal("Replica", created.Instance);
        Assert.Contains(rig.LogEntries(), line => line.Contains("Failover -> START"));
        Assert.Contains(rig.LogEntries(), line => line.Contains("Request -> через Replica"));
        Assert.Contains(rig.LogEntries(), line => line.Contains("Replication -> SKIPPED, Primary -> UNAVAILABLE"));

        int rentalId = created.Data!.Value.GetProperty("id").GetInt32();

        // Оба экземпляра недоступны — запрос не выполняется, но сервер жив и отвечает понятно.
        var both = new TestRig(primary: TestRig.UnavailableDatabase, replica: TestRig.UnavailableDatabase);
        Response dead = await both.Handler.HandleAsync(TestRig.Empty("ShowState"), clientId: 4);

        Assert.False(dead.Success);
        Assert.Equal("Недоступны оба экземпляра данных. Запрос не выполнен, сервер продолжает работу.", dead.Message);

        // Сервис перезапущен: новые экземпляры тех же классов поверх той же базы видят записи старого.
        var restarted = new TestRig();
        Response state = await restarted.Handler.HandleAsync(TestRig.Empty("ShowState"), clientId: 1);

        Assert.True(state.Success);
        Assert.Equal("Replica", state.Instance);
        Assert.Single(restarted.Replica.Database.GetRentals(),
            rental => rental.Id == rentalId && rental.Status == "Active" && rental.RegistrationNumber == TestRig.ExperimentCar);
        Assert.False(restarted.Replica.Database.IsAvailable(TestRig.ExperimentCar));

        // Копии выравниваются, и работа возвращается на Primary без потери записи.
        Response sync = await restarted.Handler.HandleAsync(TestRig.Empty("SyncData"), clientId: 2);

        Assert.True(sync.Success);
        Assert.Equal("Primary", (await restarted.Handler.HandleAsync(TestRig.Empty("ShowState"), clientId: 3)).Instance);
        Assert.Single(restarted.Primary.Database.GetRentals(), rental => rental.Id == rentalId);

        Response returned = await restarted.Handler.HandleAsync(ReturnCarRequest(rentalId), clientId: 4);

        Assert.True(returned.Success);
        Assert.True(restarted.Primary.Database.IsAvailable(TestRig.ExperimentCar));
        Assert.True(restarted.Replicator.Compare(restarted.Primary, restarted.Replica).IsConsistent);
    }

    // ---------------------------------------------------------------- вспомогательное

    private static Request SearchInMinsk()
    {
        return new Request
        {
            Operation = "SearchCar",
            Data = PubSubJson.ToElement(new { location = "Минск" })
        };
    }

    private static Request ReturnCarRequest(int rentalId)
    {
        return new Request
        {
            Operation = "ReturnCar",
            Data = PubSubJson.ToElement(new { rentalId })
        };
    }

    private static List<string> RegistrationsOf(Response response)
    {
        Assert.NotNull(response.Data);

        return response.Data!.Value.EnumerateArray()
            .Select(car => car.GetProperty("registrationNumber").GetString() ?? string.Empty)
            .ToList();
    }

    private static int RentalsOf(Response showState) => showState.Data!.Value.GetProperty("rentals").GetArrayLength();

    private static List<string> LinesOf(Response response)
    {
        Assert.NotNull(response.Data);

        return response.Data!.Value.EnumerateArray().Select(item => item.GetString() ?? string.Empty).ToList();
    }

    /// LineConnection пишет только сериализованный JSON, а проверять надо и пустую строку, и мусор:
    /// здесь одна строка текста уходит в поток как есть, чтобы сервер прошёл свои же развилки в Program.cs.
    private static async Task<Response> AskRawLineAsync(string line)
    {
        using var client = new TcpClient { NoDelay = true };

        await client.ConnectAsync(ContainerStand.Host, ContainerStand.ServerPort)
            .WaitAsync(TimeSpan.FromMilliseconds(1500));

        using var writer = new StreamWriter(client.GetStream()) { AutoFlush = true };
        using var reader = new StreamReader(client.GetStream());
        using var timeout = new CancellationTokenSource(30000);

        await writer.WriteLineAsync(line);

        string? answer = await reader.ReadLineAsync(timeout.Token);

        if (answer == null)
        {
            throw new PubSubTransportException($"сервер не ответил на строку '{line}'");
        }

        return PubSubJson.Deserialize<Response>(answer)
               ?? throw new InvalidOperationException($"сервер ответил не сообщением: {answer}");
    }

    /// Ожидание с таймаутом: подписчик получает событие асинхронно, а вешать тест нельзя.
    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 10000)
    {
        var watch = System.Diagnostics.Stopwatch.StartNew();

        while (!condition())
        {
            if (watch.ElapsedMilliseconds > timeoutMs)
            {
                throw new TimeoutException("условие не наступило за отведённое время");
            }

            await Task.Delay(50);
        }
    }
}
