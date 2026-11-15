using Npgsql;
using Server;
using Shared.Cluster;
using Shared.Events;
using Shared.Protocol;

namespace Tests;

/// <summary>
/// Значения переменных стенда из labs/lab8/compose.yaml. Один источник для тестов: проверки
/// ссылаются на те же строки, которые сервер читает из environment, и меняются вместе с файлом.
/// Внутри сети riot-lab8 адреса — имена сервисов (primary, replica, broker, node1…node5),
/// localhost в них отсутствует.
/// </summary>
internal static class Compose
{
    public const string ClusterMembers = "1=node1:6001,2=node2:6002,3=node3:6003,4=node4:6004,5=node5:6005";
    public const string Brokers = "broker:5001";
    public const string PrimaryConnection =
        "Host=primary;Port=5432;Username=postgres;Password=postgres;Database=carrental;Timeout=5";
    public const string ReplicaConnection =
        "Host=replica;Port=5432;Username=postgres;Password=postgres;Database=carrental;Timeout=5";
}

/// <summary>
/// Проверка конфигурации контейнеризованного стенда: разбор CLUSTER_MEMBERS, PUBLISHER_BROKERS
/// и строк подключения к PostgreSQL теми же публичными методами, которые вызывает Server/Program.cs
/// при старте. Тесты не обращаются к приватным членам и не требуют нового публичного API.
/// Docker здесь не нужен: проверяется, что приложение понимает адреса из compose.yaml.
/// </summary>
public class ContainerConfigTests
{
    // ---------------------------------------------------------------- CLUSTER_MEMBERS

    [Fact]
    public void MemberParse_ComposeClusterMembers_GivesFiveNodesByServiceName()
    {
        List<Member> members = Member.Parse(Compose.ClusterMembers);

        Assert.Equal(5, members.Count);
        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, members.Select(member => member.Id).ToArray());
        Assert.Equal(new[] { "node1", "node2", "node3", "node4", "node5" }, members.Select(member => member.Host).ToArray());
        Assert.Equal(new[] { 6001, 6002, 6003, 6004, 6005 }, members.Select(member => member.Port).ToArray());

        // Имена в журнале — «Node 5», а адрес — имя сервиса: наружу из контейнера он не годится.
        Assert.Equal("Node 1", members[0].Name);
        Assert.Equal("node5:6005", members[4].Endpoint);
        Assert.DoesNotContain(members, member => member.Host is "127.0.0.1" or "localhost");
    }

    [Theory]
    [InlineData("1=node1:6001,1=node2:6002")]
    [InlineData("node1:6001")]
    [InlineData("1=node1")]
    [InlineData("1=node1:")]
    [InlineData("1=node1:port")]
    [InlineData("x=node1:6001")]
    [InlineData(",,,")]
    public void MemberParse_ComposeLikeSettingWithDefect_ThrowsFormatException(string setting)
    {
        FormatException error = Assert.Throws<FormatException>(() => Member.Parse(setting));

        Assert.Contains("CLUSTER_MEMBERS", error.Message);
    }

    [Fact]
    public void MemberParse_SameEndpointUnderDifferentIds_IsAccepted()
    {
        // Разбор проверяет формат и уникальность id. Два узла на одном адресе — ошибка compose.yaml,
        // а не парсера: её видно по журналу узлов, когда они не могут выбрать лидера.
        Assert.Equal(2, Member.Parse("1=node1:6001,2=node1:6001").Count);
    }

    [Fact]
    public void Cluster_ComposeMembers_CountsMajorityOfFiveNodes()
    {
        var journal = new Journal();
        var cluster = new Cluster(Compose.ClusterMembers, journal);

        Assert.True(cluster.Enabled);
        Assert.Equal(5, cluster.TotalNodes);
        Assert.Equal(3, cluster.Majority);
        Assert.Equal("node3:6003", cluster.Members[2].Endpoint);
    }

    [Fact]
    public void Cluster_DefectiveMembersSetting_ThrowsBeforeServerStarts()
    {
        // Server/Program.cs ловит FormatException и продолжает работу без кластера — здесь только сам разбор.
        Assert.Throws<FormatException>(() => new Cluster("1=node1:6001,1=node2:6002", new Journal()));
    }

    [Fact]
    public void Cluster_EmptyMembersSetting_IsDisabledWithoutThrowing()
    {
        Assert.False(new Cluster(string.Empty, new Journal()).Enabled);
        Assert.False(new Cluster("   ", new Journal()).Enabled);
        Assert.False(new Cluster(null, new Journal()).Enabled);
    }

    // ---------------------------------------------------------------- PUBLISHER_BROKERS

    [Fact]
    public void Publisher_ComposeBrokerAddress_ParsedAsServiceName()
    {
        var journal = new Journal();
        var publisher = new Publisher(Compose.Brokers, journal);

        BrokerEndpoint broker = Assert.Single(publisher.Endpoints);
        Assert.Equal("broker", broker.Host);
        Assert.Equal(5001, broker.Port);
        Assert.Equal("broker:5001", broker.Label);
        Assert.Empty(journal.Recent(10));
    }

    [Fact]
    public void Publisher_TwoBrokersListFromCompose_KeepsOrderAndBothAddresses()
    {
        var publisher = new Publisher("broker:5001,broker2:5002", new Journal());

        Assert.Equal(2, publisher.Endpoints.Count);
        Assert.Equal("broker:5001", publisher.Endpoints[0].Label);
        Assert.Equal("broker2:5002", publisher.Endpoints[1].Label);
    }

    [Theory]
    [InlineData("broker:5001,broker")]
    [InlineData("broker:5001,broker:nope")]
    [InlineData("broker:5001,broker:0")]
    [InlineData("broker:5001,broker:70000")]
    public void Publisher_MalformedBrokerAddress_IsSkippedAndNoticedInJournal(string setting)
    {
        var journal = new Journal();
        var publisher = new Publisher(setting, journal);

        BrokerEndpoint broker = Assert.Single(publisher.Endpoints);
        Assert.Equal("broker:5001", broker.Label);

        string bad = setting.Split(',')[1];
        Assert.Contains(journal.Recent(20), line => line.Contains($"адрес Broker '{bad}' некорректен — пропущен"));
    }

    [Fact]
    public void Publisher_SettingWithoutAnyValidAddress_FallsBackToLoopback()
    {
        // Важно для понимания гейта: «выключенный» Publisher делает Program (PUBLISHER_BROKERS пуст),
        // сам Publisher никогда не остаётся без адреса — иначе он бил бы в пустоту молча.
        var journal = new Journal();
        var publisher = new Publisher("broker;brokerx", journal);

        BrokerEndpoint broker = Assert.Single(publisher.Endpoints);
        Assert.Equal("127.0.0.1", broker.Host);
        Assert.Equal(5001, broker.Port);
        Assert.Contains(journal.Recent(20), line =>
            line.Contains("ни один адрес Broker не разобран, использован 127.0.0.1:5001"));
    }

    [Fact]
    public void Publisher_BrokerHostOfComposeNetwork_IsNotResolvableFromHost()
    {
        var publisher = new Publisher(Compose.Brokers, new Journal());
        List<PublishResultMessage> results = publisher.Publish(new DomainEvent
        {
            EventType = EventTypes.RentalCreated,
            EntityId = 1
        });

        Assert.Equal("BROKER_UNAVAILABLE", Assert.Single(results).Status);
    }

    // ---------------------------------------------------------------- CARRENTAL_*_CONNECTION

    [Fact]
    public void ConnectionString_ComposePrimary_ParsesWithHostEqualToServiceName()
    {
        var builder = new NpgsqlConnectionStringBuilder(Compose.PrimaryConnection);

        Assert.Equal("primary", builder.Host);
        Assert.Equal(5432, builder.Port);
        Assert.Equal("carrental", builder.Database);
        Assert.Equal("postgres", builder.Username);
        Assert.Equal(5, builder.Timeout);

        // Внутри контейнера localhost — это сам контейнер, поэтому имя сервиса обязательно.
        Assert.DoesNotContain("127.0.0.1", Compose.PrimaryConnection);
        Assert.DoesNotContain("localhost", Compose.PrimaryConnection);
    }

    [Fact]
    public void ConnectionString_ComposeReplica_DiffersFromPrimaryOnlyByHost()
    {
        var primary = new NpgsqlConnectionStringBuilder(Compose.PrimaryConnection);
        var replica = new NpgsqlConnectionStringBuilder(Compose.ReplicaConnection);

        Assert.Equal("replica", replica.Host);
        Assert.NotEqual(primary.Host, replica.Host);
        Assert.Equal(primary.Port, replica.Port);
        Assert.Equal(primary.Database, replica.Database);
        Assert.Equal(primary.Username, replica.Username);
    }

    [Fact]
    public void CarRentalDatabase_ComposeConnectionString_PingReturnsFalseOutsideTheNetwork()
    {
        var database = new CarRentalDatabase("Primary", Compose.PrimaryConnection);

        // Сервер стартует с этим же объектом: неготовность БД должна быть состоянием, а не исключением.
        Assert.False(database.Ping());
        Assert.Equal(Compose.PrimaryConnection, database.ConnectionString);
    }

    // ---------------------------------------------------------------- значения по умолчанию

    [Fact]
    public void Database_DefaultConnections_UsePortsPublishedByCompose()
    {
        var primary = new NpgsqlConnectionStringBuilder(CarRentalDatabase.DefaultPrimaryConnectionString);
        var replica = new NpgsqlConnectionStringBuilder(CarRentalDatabase.DefaultReplicaConnectionString);

        // Хостовые значения по умолчанию (5433/5434) совпадают с ports из compose.yaml: запуск на хосте
        // и тесты без Docker работают по ним же.
        Assert.Equal(5433, primary.Port);
        Assert.Equal(5434, replica.Port);
        Assert.Equal("127.0.0.1", primary.Host);
        Assert.Equal("carrental", primary.Database);
        Assert.Equal("carrental", replica.Database);
    }

    [Fact]
    public async Task Server_WithEmptyClusterAndBrokerSettings_WorksWithoutBothExtensions()
    {
        TestRig.ResetPair();

        var rig = new TestRig();
        var handler = new RequestHandler(
            rig.Instances,
            rig.Replicator,
            rig.Journal,
            processingDelayMs: 0,
            publisher: null,
            cluster: new Cluster(string.Empty, rig.Journal));

        Response search = await handler.HandleAsync(TestRig.Empty("SearchCar"), clientId: 1);
        Response status = await handler.HandleAsync(TestRig.Empty("ClusterStatus"), clientId: 1);
        Response created = await handler.HandleAsync(
            TestRig.CreateRental("Иван Петров", "375291234567", TestRig.ExperimentCar, clientName: "Settings"),
            clientId: 1);

        Assert.True(search.Success);
        Assert.True(created.Success);

        // CLUSTER_MEMBERS пуст — сервер работает как в ЛР №6: консенсус не запрашивается.
        Assert.False(status.Success);
        Assert.Contains("CLUSTER_MEMBERS пуст", status.Message);
        Assert.Null(created.Consensus);

        // PUBLISHER_BROKERS пуст — события нет, но запрос выполнен (строка журнала от RequestHandler).
        Assert.Null(created.Events);
        Assert.Contains(rig.LogEntries(), line => line.Contains("НЕ ОПУБЛИКОВАН"));
        Assert.DoesNotContain(rig.LogEntries(), line => line.Contains("Consensus ->"));
    }
}
