using System.Text.Json;
using Server;
using Shared.Protocol;

namespace Tests;

/// <summary>
/// Пара экземпляров данных для тестов: тот же код, что поднимает сервер.
/// Недоступный экземпляр моделируется строкой подключения к свободному порту — сценарий тот же,
/// что и у остановленного контейнера: Ping() не проходит, экземпляр помечается UNAVAILABLE.
/// </summary>
internal sealed class TestRig
{
    public const string PrimaryDatabase =
        "Host=127.0.0.1;Port=5433;Username=postgres;Password=postgres;Database=carrental_test;Timeout=5;Application Name=lab6-tests-primary";

    public const string ReplicaDatabase =
        "Host=127.0.0.1;Port=5434;Username=postgres;Password=postgres;Database=carrental_test;Timeout=5;Application Name=lab6-tests-replica";

    public const string UnavailableDatabase =
        "Host=127.0.0.1;Port=5999;Username=postgres;Password=postgres;Database=carrental_test;Timeout=1";

    public const string ExperimentCar = "1234 AB-7";
    public const string SecondCar = "2345 BC-7";

    public TestRig(
        string? primary = PrimaryDatabase,
        string? replica = ReplicaDatabase,
        int processingDelayMs = 0)
    {
        Journal = new Journal();
        Primary = new DataInstance("Primary", new CarRentalDatabase("Primary", primary));
        Replica = new DataInstance("Replica", new CarRentalDatabase("Replica", replica));
        Instances = new InstanceManager(Primary, Replica, Journal);
        Replicator = new Replicator(Journal);
        Handler = new RequestHandler(Instances, Replicator, Journal, processingDelayMs);
    }

    public Journal Journal { get; }

    public DataInstance Primary { get; }

    public DataInstance Replica { get; }

    public InstanceManager Instances { get; }

    public Replicator Replicator { get; }

    public RequestHandler Handler { get; }

    /// Исходное состояние обеих баз тестов: одинаковые строки и одинаковая версия данных.
    public static void ResetPair()
    {
        var rig = new TestRig();
        rig.Primary.Database.EnsureCreated();
        rig.Replica.Database.EnsureCreated();
        rig.Primary.Database.Reset();
        rig.Replicator.FullSync(rig.Primary, rig.Replica, "Test");
        rig.Primary.Refresh();
        rig.Replica.Refresh();
    }

    public List<string> LogEntries() => Journal.Recent(200);

    public static Request CreateRental(
        string fullName,
        string phone,
        string registrationNumber,
        bool simulateError = false,
        string? clientName = null,
        int days = 3,
        bool withoutTransaction = false)
    {
        return new Request
        {
            Operation = withoutTransaction ? "CreateRentalNoTransaction" : "CreateRental",
            Data = JsonSerializer.SerializeToElement(new
            {
                fullName,
                phone,
                registrationNumber,
                startDate = new DateTime(2026, 10, 11),
                days,
                simulateError,
                clientName
            })
        };
    }

    public static Request Empty(string operation)
    {
        return new Request
        {
            Operation = operation,
            Data = JsonSerializer.SerializeToElement(new { })
        };
    }
}
