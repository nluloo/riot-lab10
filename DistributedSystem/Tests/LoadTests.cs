using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Npgsql;
using Server;
using Shared.Protocol;
using Xunit.Abstractions;

namespace Tests;

/// <summary>
/// Нагрузочное тестирование (Требование 5 Части 4): конкуренция за общий ресурс и замер
/// латентности/производительности. Два теста in-process через TestRig по настоящим базам и один
/// вариант против контейнеризованного сервера по TCP (гейт RUN_CONTAINER_TESTS=1).
/// Числа замеров в проверки не входят — они пишутся в .temp/load-inprocess.txt и .temp/load-container.txt.
/// </summary>
public class LoadTests
{
    private const int Contenders = 20;
    private const int Clients = 10;

    /// Своя машина на каждого клиента: в начальных данных всего три свободных автомобиля,
    /// иначе 10 клиентов стояли бы в очереди за ними и тест мерил бы блокировку, а не параллельность.
    private static readonly string[] LoadCars =
        Enumerable.Range(1, Clients).Select(index => $"L{index:D2}0 XY-8").ToArray();

    private readonly ITestOutputHelper _output;

    public LoadTests(ITestOutputHelper output)
    {
        _output = output;

        TestRig.ResetPair();
    }

    [Fact]
    public async Task Load_ParallelCreateRental_SameCar_ExactlyOneSucceeds()
    {
        var rig = new TestRig();

        var watch = Stopwatch.StartNew();

        Response[] responses = await InParallel(Contenders, index =>
            rig.Handler.HandleAsync(
                    TestRig.CreateRental($"Клиент {index}", Phone(index), TestRig.ExperimentCar, clientName: $"Load {index}"),
                    clientId: index + 1)
                .GetAwaiter()
                .GetResult());

        watch.Stop();

        Assert.Equal(Contenders, responses.Length);
        Assert.Equal(1, responses.Count(item => item.Success));
        Assert.Equal(Contenders - 1, responses.Count(item => item.Message == "Автомобиль уже арендован"));

        // Инвариант проверяется по базе, а не только по ответам: машина занята ровно одной активной арендой.
        Assert.Equal(1, rig.Primary.Database.CountRentals());
        Assert.Equal(1, ActiveRentals(rig.Primary.Database, TestRig.ExperimentCar));
        Assert.False(rig.Primary.Database.IsAvailable(TestRig.ExperimentCar));
        Assert.True(rig.Replicator.Compare(rig.Primary, rig.Replica).IsConsistent);

        string title = "Конкуренция: 20 параллельных CreateRental на один автомобиль";
        string[] lines =
        {
            $"requests={Contenders}  ok={responses.Count(item => item.Success)} " +
            $"rejected={responses.Count(item => !item.Success)}",
            $"wallMs={watch.ElapsedMilliseconds}  throughput={Rate(Contenders, watch.ElapsedMilliseconds)} req/s",
            $"активных аренд у {TestRig.ExperimentCar}: {ActiveRentals(rig.Primary.Database, TestRig.ExperimentCar)} (ожидалось 1)",
            $"carrental_test: rentals={rig.Primary.Database.CountRentals()} customers={rig.Primary.Database.CountCustomers()}",
            $"lsn={rig.Primary.Database.GetLsn()}"
        };

        LoadReport.Section(LoadReport.InTemp(LoadReport.InProcessFile), title, lines);
        foreach (string line in lines)
        {
            _output.WriteLine(line);
        }
    }

    [Fact]
    public async Task Load_MixedOperations_ThroughputAndLatency()
    {
        var rig = new TestRig();

        SeedLoadCars(rig);

        var watch = Stopwatch.StartNew();

        List<Sample>[] perClient = await InParallel(Clients, index => RunClient(rig, index));

        watch.Stop();

        List<Sample> samples = perClient.SelectMany(items => items).ToList();

        Assert.Equal(Clients * 4, samples.Count);
        Assert.Equal(samples.Count, samples.Count(item => item.Response != null));
        Assert.Equal(Clients * 4, samples.Count(item => item.Response.Success));

        // Двойного бронирования нет: на одну машину не больше одной активной аренды.
        Assert.Empty(rig.Primary.Database.GetRentals()
            .Where(rental => rental.Status == "Active")
            .GroupBy(rental => rental.RegistrationNumber)
            .Where(group => group.Count() > 1));

        List<int> sortedLatency = samples.Select(item => item.Milliseconds).OrderBy(value => value).ToList();

        string[] lines =
        {
            $"clients={Clients} operationsPerClient=4 requests={samples.Count} " +
            $"ok={samples.Count(item => item.Response.Success)} errors={samples.Count(item => !item.Response.Success)}",
            $"wallMs={watch.ElapsedMilliseconds} throughput={Rate(samples.Count, watch.ElapsedMilliseconds)} req/s",
            $"latencyMs overall: {LatencyLine(sortedLatency)}",
            string.Empty,
            Row("operation", "count", "avg", "p50", "p95", "max"),
            Row("ВСЕГО", sortedLatency.Count, Average(sortedLatency), Percentile(sortedLatency, 0.50),
                Percentile(sortedLatency, 0.95), sortedLatency.Max())
        };

        foreach (IGrouping<string, Sample> group in samples.GroupBy(item => item.Operation).OrderBy(g => g.Key))
        {
            List<int> values = group.Select(item => item.Milliseconds).OrderBy(value => value).ToList();

            lines = lines.Append(Row(group.Key, values.Count, Average(values), Percentile(values, 0.50),
                Percentile(values, 0.95), values.Max())).ToArray();
        }

        LoadReport.Section(LoadReport.InTemp(LoadReport.InProcessFile),
            "Смешанные операции: 10 клиентов x 4 операции (SearchCar, CreateRental, ShowState, ReturnCar)", lines);

        foreach (string line in lines)
        {
            _output.WriteLine(line);
        }
    }

    [Fact]
    public async Task Load_ContainerizedServer_MixedOperations()
    {
        if (!ContainerStand.Ready(_output))
        {
            return;
        }

        var watch = Stopwatch.StartNew();

        List<Sample>[] perClient = await InParallel(Clients, index => RunContainerClient(index));

        watch.Stop();

        List<Sample> samples = perClient.SelectMany(items => items).ToList();

        Assert.Equal(Clients * 3, samples.Count);
        Assert.Equal(Clients * 3, samples.Count(item => item.Response.Success));

        List<int> sortedLatency = samples.Select(item => item.Milliseconds).OrderBy(value => value).ToList();

        var lines = new List<string>
        {
            $"стенд: docker compose up -d, адрес {ContainerStand.Host}:{ContainerStand.ServerPort}",
            $"clients={Clients} operationsPerClient=3 requests={samples.Count} " +
            $"ok={samples.Count(item => item.Response.Success)} errors={samples.Count(item => !item.Response.Success)}",
            $"wallMs={watch.ElapsedMilliseconds} throughput={Rate(samples.Count, watch.ElapsedMilliseconds)} req/s",
            $"latencyMs overall: {LatencyLine(sortedLatency)}",
            string.Empty,
            Row("operation", "count", "avg", "p50", "p95", "max"),
            Row("ВСЕГО", sortedLatency.Count, Average(sortedLatency), Percentile(sortedLatency, 0.50),
                Percentile(sortedLatency, 0.95), sortedLatency.Max())
        };

        foreach (IGrouping<string, Sample> group in samples.GroupBy(item => item.Operation).OrderBy(g => g.Key))
        {
            List<int> values = group.Select(item => item.Milliseconds).OrderBy(value => value).ToList();

            lines.Add(Row(group.Key, values.Count, Average(values), Percentile(values, 0.50),
                Percentile(values, 0.95), values.Max()));
        }

        LoadReport.Section(LoadReport.InTemp(LoadReport.ContainerFile),
            "Контейнеризованный стенд: 10 клиентов x 3 операции по TCP", lines);

        foreach (string line in lines)
        {
            _output.WriteLine(line);
        }
    }

    /// Один клиент идёт по своим операциям последовательно: параллельность между клиентами,
    /// а внутри клиента порядок важен (аренду надо создать, прежде чем вернуть).
    private static List<Sample> RunClient(TestRig rig, int index)
    {
        var samples = new List<Sample>
        {
            Timed(rig.Handler, new Request
            {
                Operation = "SearchCar",
                Data = JsonSerializer.SerializeToElement(new { location = "Минск" })
            }, index + 1)
        };

        Sample created = Timed(rig.Handler,
            TestRig.CreateRental($"Клиент {index}", Phone(index), LoadCars[index], clientName: $"Load {index}"),
            index + 1);

        samples.Add(created);
        samples.Add(Timed(rig.Handler, TestRig.Empty("ShowState"), index + 1));

        int rentalId = created.Response.Data!.Value.GetProperty("id").GetInt32();

        samples.Add(Timed(rig.Handler, new Request
        {
            Operation = "ReturnCar",
            Data = JsonSerializer.SerializeToElement(new { rentalId })
        }, index + 1));

        return samples;
    }

    /// Только операции чтения: замеры против стенда не должны портить демо-данные базы carrental.
    private static List<Sample> RunContainerClient(int index)
    {
        return new List<Sample>
        {
            TimedContainer(new Request
            {
                Operation = "SearchCar",
                Data = JsonSerializer.SerializeToElement(new { location = "Минск", brandOrModel = "a" })
            }),
            TimedContainer(TestRig.Empty("ShowState")),
            TimedContainer(TestRig.Empty("ShowInstances"))
        };
    }

    private static Sample Timed(RequestHandler handler, Request request, int clientId)
    {
        var watch = Stopwatch.StartNew();
        Response response = handler.HandleAsync(request, clientId).GetAwaiter().GetResult();
        watch.Stop();

        return new Sample(request.Operation, (int)watch.ElapsedMilliseconds, response);
    }

    private static Sample TimedContainer(Request request)
    {
        var watch = Stopwatch.StartNew();
        Response response = ContainerStand.Ask(request);
        watch.Stop();

        return new Sample(request.Operation, (int)watch.ElapsedMilliseconds, response);
    }

    /// Добавляем машины в Primary и передаём их в Replica, чтобы копии остались согласованы:
    /// без этого сверка копий в конце показала бы расхождение, которого в стенде нет.
    private static void SeedLoadCars(TestRig rig)
    {
        using (var connection = rig.Primary.Database.Open())
        using (var command = new NpgsqlCommand(
                   """
                   INSERT INTO cars (brand, model, registration_number, location_id, daily_price, is_available)
                   VALUES ('Load', 'Station Wagon', @registration, 1, 50, TRUE)
                   ON CONFLICT (registration_number) DO NOTHING
                   """,
                   connection))
        {
            foreach (string registration in LoadCars)
            {
                command.Parameters.Clear();
                command.Parameters.AddWithValue("registration", registration);
                command.ExecuteNonQuery();
            }
        }

        rig.Primary.Database.BumpLsn();
        rig.Replicator.FullSync(rig.Primary, rig.Replica, "Load");
        rig.Primary.Refresh();
        rig.Replica.Refresh();
    }

    private static int ActiveRentals(CarRentalDatabase database, string registration) =>
        database.GetRentals().Count(rental => rental.Status == "Active" && rental.RegistrationNumber == registration);

    private static string Phone(int index) => $"375291000{index:D3}";

    /// Каждый клиент — свой выделенный поток, старт по общему сигналу: иначе запросы шли бы
    /// последовательно на одном потоке и никакой нагрузки стенд бы не получил.
    private static async Task<T[]> InParallel<T>(int count, Func<int, T> action)
    {
        using var start = new ManualResetEventSlim(false);
        var factory = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

        Task<T>[] tasks = Enumerable.Range(0, count)
            .Select(index => factory.StartNew(() =>
            {
                start.Wait();
                return action(index);
            }))
            .ToArray();

        start.Set();

        return await Task.WhenAll(tasks);
    }

    private static string Row(params object[] cells) =>
        string.Format(CultureInfo.InvariantCulture, "{0,-16}{1,6}{2,8}{3,6}{4,6}{5,6}", cells);

    private static double Average(List<int> sorted) => Math.Round(sorted.Average(), 1, MidpointRounding.AwayFromZero);

    /// Строка квантилей: разделитель — точка, чтобы числа из файла шли в xlsx без правок.
    private static string LatencyLine(List<int> sorted) =>
        FormattableString.Invariant(
            $"avg={Average(sorted)} p50={Percentile(sorted, 0.50)} p95={Percentile(sorted, 0.95)} max={sorted.Max()}");

    /// Процентиль по уже отсортированному списку — нужен для отчёта, а не для проверок.
    private static int Percentile(List<int> sorted, double fraction)
    {
        int index = (int)Math.Round(sorted.Count * fraction, MidpointRounding.AwayFromZero) - 1;

        return sorted[Math.Clamp(index, 0, sorted.Count - 1)];
    }

    private static string Rate(int requests, long milliseconds) =>
        Math.Round(requests * 1000d / Math.Max(1, milliseconds), 1, MidpointRounding.AwayFromZero)
            .ToString(CultureInfo.InvariantCulture);
}

/// Одна измеренная операция: латентность в мс и ответ сервера.
internal sealed record Sample(string Operation, int Milliseconds, Response Response);

/// <summary>
/// Файл с числами замеров: их вставляют в README и xlsx. Первый вызов для пути затирает файл,
/// остальные только дописывают секцию — содержимое не зависит от порядка запуска тестов.
/// </summary>
internal static class LoadReport
{
    public const string InProcessFile = "load-inprocess.txt";
    public const string ContainerFile = "load-container.txt";

    private static readonly object Sync = new();
    private static readonly HashSet<string> Started = new();
    private static readonly Encoding Utf8WithoutBom = new UTF8Encoding(false);

    /// Каталог .temp в корне репозитория: поднимаемся от bin до каталога labs, его родитель — корень.
    /// Ищем именно labs, а не .git: .git в этом дереве выше (один репозиторий на несколько курсов),
    /// а числа нужны рядом с labs/lab8/README.
    public static string InTemp(string fileName)
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);

        while (directory != null && directory.Name != "labs")
        {
            directory = directory.Parent;
        }

        string root = directory?.Parent?.FullName ?? AppContext.BaseDirectory;
        string temp = Path.Combine(root, ".temp");
        Directory.CreateDirectory(temp);

        return Path.Combine(temp, fileName);
    }

    public static void Section(string path, string title, IEnumerable<string> lines)
    {
        lock (Sync)
        {
            if (Started.Add(path))
            {
                File.WriteAllText(path,
                    $"Замеры нагрузки, {DateTime.Now:yyyy-MM-dd HH:mm:ss} (получены тестами Tests/LoadTests.cs)"
                    + Environment.NewLine + Environment.NewLine,
                    Utf8WithoutBom);
            }

            File.AppendAllText(path,
                $"=== {title} ==={Environment.NewLine}" +
                string.Join(Environment.NewLine, lines) +
                Environment.NewLine + Environment.NewLine,
                Utf8WithoutBom);
        }
    }
}
