using System.Text.Json;
using Npgsql;
using Server;
using Shared.Protocol;

namespace Tests;

/// Тесты репликации, отказоустойчивости и восстановления из ЛР №5.
public class ReplicationTests
{
    private const string ExperimentCar = TestRig.ExperimentCar;

    public ReplicationTests()
    {
        TestRig.ResetPair();
    }

    [Fact]
    public void InitialState_CopiesAreIdentical()
    {
        var rig = new TestRig();
        DiffReport report = rig.Replicator.Compare(rig.Primary, rig.Replica);

        Assert.True(report.IsConsistent);
        Assert.Equal(report.PrimaryLsn, report.ReplicaLsn);
        Assert.Empty(report.Differences);
    }

    [Fact]
    public async Task CreateRental_ChangesAreReplicatedToReplica()
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Иванов Иван", "375291111111", ExperimentCar, clientName: "Client 1"));

        Assert.True(response.Success);
        Assert.Equal("Primary", response.Instance);

        Assert.Equal(1, rig.Replica.Database.CountRentals());
        Assert.False(rig.Replica.Database.IsAvailable(ExperimentCar));

        DiffReport after = rig.Replicator.Compare(rig.Primary, rig.Replica);
        Assert.True(after.IsConsistent);
        Assert.Contains(rig.LogEntries(), line => line.Contains("Replication -> SUCCESS"));
    }

    [Fact]
    public async Task CreateRental_VersionOfBothCopiesGrowsByOne()
    {
        var rig = new TestRig();
        long before = rig.Primary.Database.GetLsn();

        await rig.Handler.HandleAsync(
            TestRig.CreateRental("Иванов Иван", "375291111111", ExperimentCar));

        long after = rig.Primary.Database.GetLsn();

        Assert.Equal(before + 1, after);
        Assert.Equal(after, rig.Replica.Database.GetLsn());
    }

    [Fact]
    public async Task ReturnCar_ChangesAreReplicatedToReplica()
    {
        var rig = new TestRig();
        Response created = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Иванов Иван", "375291111111", ExperimentCar));
        int rentalId = created.Data!.Value.GetProperty("id").GetInt32();

        Response returned = await rig.Handler.HandleAsync(
            new Request
            {
                Operation = "ReturnCar",
                Data = JsonSerializer.SerializeToElement(new { rentalId })
            });

        Assert.True(returned.Success);
        Assert.True(rig.Replica.Database.IsAvailable(ExperimentCar));
        Assert.Equal("Returned", rig.Replica.Database.GetRental(rentalId)!.Status);
        Assert.True(rig.Replicator.Compare(rig.Primary, rig.Replica).IsConsistent);
    }

    [Fact]
    public async Task TransactionRollback_LeavesBothCopiesUnchanged()
    {
        var rig = new TestRig();
        long before = rig.Primary.Database.GetLsn();

        Response response = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Иванов Иван", "375291111111", ExperimentCar, simulateError: true));

        Assert.False(response.Success);
        Assert.Equal(0, rig.Primary.Database.CountRentals());
        Assert.Equal(before, rig.Primary.Database.GetLsn());
        Assert.Contains(rig.LogEntries(), line => line.Contains("Replication -> NOT NEEDED"));
        Assert.True(rig.Replicator.Compare(rig.Primary, rig.Replica).IsConsistent);
    }

    [Fact]
    public async Task FullSync_PropagatesDeletions()
    {
        var rig = new TestRig();
        await rig.Handler.HandleAsync(
            TestRig.CreateRental("Иванов Иван", "375291111111", ExperimentCar));

        Assert.Equal(1, rig.Replica.Database.CountRentals());

        using (var connection = rig.Primary.Database.Open())
        using (var delete = new NpgsqlCommand("DELETE FROM rentals", connection))
        {
            delete.ExecuteNonQuery();
        }

        rig.Replicator.FullSync(rig.Primary, rig.Replica, "Test");

        Assert.Equal(0, rig.Replica.Database.CountRentals());
        Assert.True(rig.Replicator.Compare(rig.Primary, rig.Replica).IsConsistent);
    }

    [Fact]
    public void Refresh_WhenInstanceStopped_MarksItUnavailable()
    {
        var rig = new TestRig(primary: TestRig.UnavailableDatabase);

        Assert.False(rig.Primary.Refresh());
        Assert.Equal("UNAVAILABLE", rig.Primary.StatusText);
        Assert.False(rig.Primary.IsAvailable);
    }

    [Fact]
    public void SelectForRequest_WhenPrimaryFailed_UsesReplica()
    {
        var rig = new TestRig(primary: TestRig.UnavailableDatabase);

        DataInstance? chosen = rig.Instances.SelectForRequest("Client 1");

        Assert.Same(rig.Replica, chosen);
        Assert.Contains(rig.LogEntries(), line => line.Contains("Failover -> START"));
        Assert.Contains(rig.LogEntries(), line => line.Contains("через Replica"));
    }

    [Fact]
    public async Task CreateRental_WhilePrimaryFailed_SucceedsThroughReplica()
    {
        var rig = new TestRig(primary: TestRig.UnavailableDatabase);

        Response response = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", ExperimentCar, clientName: "Client 1"));

        Assert.True(response.Success);
        Assert.Equal("Replica", response.Instance);
        Assert.Equal(1, rig.Replica.Database.CountRentals());
        Assert.False(rig.Replica.Database.IsAvailable(ExperimentCar));
        Assert.Contains(rig.LogEntries(),
            line => line.Contains("Replication -> SKIPPED, Primary -> UNAVAILABLE"));
    }

    [Fact]
    public async Task SeveralRequests_WhilePrimaryFailed_AllServedByReplica()
    {
        var rig = new TestRig(primary: TestRig.UnavailableDatabase);

        Response state = await rig.Handler.HandleAsync(TestRig.Empty("ShowState"));
        Response search = await rig.Handler.HandleAsync(
            new Request
            {
                Operation = "SearchCar",
                Data = JsonSerializer.SerializeToElement(new { location = "Минск" })
            });
        Response rental = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 2", "375291000002", TestRig.SecondCar));

        Assert.Equal("Replica", state.Instance);
        Assert.Equal("Replica", search.Instance);
        Assert.Equal("Replica", rental.Instance);
        Assert.True(search.Success);
        Assert.Equal(1, rig.Replica.Database.CountRentals());
    }

    [Fact]
    public async Task CompareData_WhilePrimaryFailed_ReturnsError()
    {
        var rig = new TestRig(primary: TestRig.UnavailableDatabase);

        Response response = await rig.Handler.HandleAsync(TestRig.Empty("CompareData"));

        Assert.False(response.Success);
        Assert.Contains("Сравнение невозможно", response.Message);
    }

    [Fact]
    public async Task SyncData_WhilePrimaryFailed_ReturnsError()
    {
        var rig = new TestRig(primary: TestRig.UnavailableDatabase);

        Response response = await rig.Handler.HandleAsync(TestRig.Empty("SyncData"));

        Assert.False(response.Success);
        Assert.Contains("Синхронизация невозможна", response.Message);
    }

    [Fact]
    public async Task Recovery_WhileCopiesDiffer_StalePrimaryIsNotUsed()
    {
        var outage = new TestRig(primary: TestRig.UnavailableDatabase);
        await outage.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", ExperimentCar));
        await outage.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 2", "375291000002", TestRig.SecondCar));

        var rig = new TestRig();
        rig.Primary.Refresh();
        rig.Replica.Refresh();

        Assert.True(rig.Replica.Lsn > rig.Primary.Lsn);

        Response compare = await rig.Handler.HandleAsync(TestRig.Empty("CompareData"));

        Assert.True(compare.Success);
        Assert.False(compare.Data!.Value.GetProperty("isConsistent").GetBoolean());

        Response state = await rig.Handler.HandleAsync(TestRig.Empty("ShowState"));

        Assert.Equal("Replica", state.Instance);
    }

    [Fact]
    public async Task Synchronization_AfterRecovery_MakesCopiesIdenticalAndReturnsToPrimary()
    {
        var outage = new TestRig(primary: TestRig.UnavailableDatabase);
        await outage.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", ExperimentCar));

        var rig = new TestRig();

        Response duringOutage = await rig.Handler.HandleAsync(TestRig.Empty("ShowState"));
        Assert.Equal("Replica", duringOutage.Instance);

        Response sync = await rig.Handler.HandleAsync(TestRig.Empty("SyncData"));

        Assert.True(sync.Success);
        Assert.Equal(1, rig.Primary.Database.CountRentals());
        Assert.True(rig.Replicator.Compare(rig.Primary, rig.Replica).IsConsistent);

        Response next = await rig.Handler.HandleAsync(TestRig.Empty("ShowState"));

        Assert.Equal("Primary", next.Instance);
        Assert.Contains(rig.LogEntries(), line => line.Contains("Failover -> OFF"));
    }

    [Fact]
    public async Task Replication_WhenReplicaFailed_ChangeStaysInPrimaryOnly()
    {
        var rig = new TestRig(replica: TestRig.UnavailableDatabase);

        Response response = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", ExperimentCar));

        Assert.True(response.Success);
        Assert.Equal("Primary", response.Instance);
        Assert.Equal(1, rig.Primary.Database.CountRentals());
        Assert.Contains(rig.LogEntries(),
            line => line.Contains("Replication -> SKIPPED, Replica -> UNAVAILABLE"));
    }

    [Fact]
    public async Task SyncData_AfterFailedReplication_UpdatesReplica()
    {
        var outage = new TestRig(replica: TestRig.UnavailableDatabase);

        await outage.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", ExperimentCar));

        var rig = new TestRig();
        Response sync = await rig.Handler.HandleAsync(TestRig.Empty("SyncData"));

        Assert.True(sync.Success);
        Assert.Equal(1, rig.Replica.Database.CountRentals());
        Assert.True(rig.Replicator.Compare(rig.Primary, rig.Replica).IsConsistent);
    }

    [Fact]
    public async Task BothCopiesUnavailable_RequestFailsWithoutThrowing()
    {
        var rig = new TestRig(primary: TestRig.UnavailableDatabase, replica: TestRig.UnavailableDatabase);

        Response rental = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", ExperimentCar));
        Response state = await rig.Handler.HandleAsync(TestRig.Empty("ShowState"));

        Assert.False(rental.Success);
        Assert.Equal("Недоступны оба экземпляра данных. Запрос не выполнен, сервер продолжает работу.",
            rental.Message);
        Assert.False(state.Success);
        Assert.Contains(rig.LogEntries(),
            line => line.Contains("Failover -> FAIL, оба экземпляра недоступны"));
    }

    [Fact]
    public async Task ShowInstances_ReportsStateAndVersionOfBothCopies()
    {
        var rig = new TestRig();
        await rig.Handler.HandleAsync(TestRig.Empty("ShowState"));

        Response response = await rig.Handler.HandleAsync(TestRig.Empty("ShowInstances"));

        Assert.True(response.Success);
        Assert.Equal("Primary", response.Data!.Value.GetProperty("active").GetString());

        JsonElement instances = response.Data.Value.GetProperty("instances");
        Assert.Equal(2, instances.GetArrayLength());
        Assert.Equal("Primary", instances[0].GetProperty("name").GetString());
        Assert.Equal("AVAILABLE", instances[0].GetProperty("state").GetString());
        Assert.True(instances[0].GetProperty("lsn").GetInt64() >= 1);
        Assert.Equal(4, instances[0].GetProperty("cars").GetArrayLength());
        Assert.Equal("Replica", instances[1].GetProperty("name").GetString());
    }

    [Fact]
    public async Task ReplicationLog_ReturnsRecentServerEvents()
    {
        var rig = new TestRig();
        await rig.Handler.HandleAsync(
            TestRig.CreateRental("Иванов Иван", "375291111111", ExperimentCar));

        Response response = await rig.Handler.HandleAsync(TestRig.Empty("ReplicationLog"));

        Assert.True(response.Success);
        List<string?> entries = response.Data!.Value.EnumerateArray()
            .Select(item => item.GetString())
            .ToList();

        Assert.Contains(entries, line => line != null && line.Contains("Replication -> SUCCESS"));
        Assert.Contains(entries, line => line != null && line.Contains("Replica -> UPDATE"));
    }

    [Fact]
    public async Task ResetData_SynchronizesBothCopies()
    {
        var rig = new TestRig();
        await rig.Handler.HandleAsync(
            TestRig.CreateRental("Иванов Иван", "375291111111", ExperimentCar));

        Response reset = await rig.Handler.HandleAsync(TestRig.Empty("ResetData"));

        Assert.True(reset.Success);
        Assert.Equal(0, rig.Primary.Database.CountRentals());
        Assert.Equal(0, rig.Replica.Database.CountRentals());
        Assert.True(rig.Primary.Database.IsAvailable(ExperimentCar));
        Assert.True(rig.Replica.Database.IsAvailable(ExperimentCar));
        Assert.True(rig.Replicator.Compare(rig.Primary, rig.Replica).IsConsistent);
    }

    [Fact]
    public async Task ResetData_WhileReplicaFailed_LeavesReplicaBehind()
    {
        var rig = new TestRig(replica: TestRig.UnavailableDatabase);

        Response reset = await rig.Handler.HandleAsync(TestRig.Empty("ResetData"));

        Assert.True(reset.Success);
        Assert.Contains("синхронизация отложена", reset.Message);
        Assert.Contains(rig.LogEntries(),
            line => line.Contains("Replication -> SKIPPED, Replica -> UNAVAILABLE"));
    }

    [Fact]
    public async Task Monitor_DetectsInstanceFailure()
    {
        var rig = new TestRig(primary: TestRig.UnavailableDatabase);
        using var token = new CancellationTokenSource();

        Task monitor = rig.Instances.MonitorAsync(50, token.Token);
        await Task.Delay(300);
        token.Cancel();
        await monitor;

        Assert.Contains(rig.LogEntries(), line => line.Contains("Heartbeat -> Primary -> UNAVAILABLE"));
    }
}
