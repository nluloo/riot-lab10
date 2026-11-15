using System.Diagnostics;
using System.Net.Sockets;
using System.Text.Json;
using Npgsql;
using Server;
using Shared.Protocol;

namespace Tests;

/// Тесты обработки запросов, унаследованные из ЛР №4: транзакции, lock и ошибки.
/// В ЛР №5 каждая проверка читает результат ещё и из Replica — изменение не должно
/// оставлять вторую копию в расходящемся состоянии.
public class RequestHandlerTests
{
    private const string ExperimentCar = TestRig.ExperimentCar;

    public RequestHandlerTests()
    {
        TestRig.ResetPair();
    }

    [Fact]
    public async Task SearchCar_ReadsAvailableCarsFromActiveInstance()
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(new Request
        {
            Operation = "SearchCar",
            Data = JsonSerializer.SerializeToElement(new { location = "Минск", brandOrModel = "Toyota" })
        });

        Assert.True(response.Success);
        Assert.Equal("Primary", response.Instance);
        Assert.NotNull(response.Data);
        Assert.Equal(1, response.Data.Value.GetArrayLength());
        Assert.Equal(ExperimentCar, response.Data.Value[0].GetProperty("registrationNumber").GetString());
    }

    [Fact]
    public async Task CreateRental_WithTransaction_CommitsRentalAndCar()
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Иванов Иван", "375291111111", ExperimentCar));

        Assert.True(response.Success);
        Assert.Contains("COMMIT", response.Message);
        Assert.Equal(1, rig.Primary.Database.CountRentals());
        Assert.Equal(1, rig.Primary.Database.CountCustomers());
        Assert.False(rig.Primary.Database.IsAvailable(ExperimentCar));

        Assert.Equal(1, rig.Replica.Database.CountRentals());
        Assert.Equal(1, rig.Replica.Database.CountCustomers());
        Assert.False(rig.Replica.Database.IsAvailable(ExperimentCar));
    }

    [Fact]
    public async Task CreateRental_WithoutTransactionAndWithoutError_SavesBothOperations()
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Иванов Иван", "375291111111", ExperimentCar, withoutTransaction: true));

        Assert.True(response.Success);
        Assert.Equal(1, rig.Primary.Database.CountRentals());
        Assert.False(rig.Primary.Database.IsAvailable(ExperimentCar));
        Assert.True(rig.Replicator.Compare(rig.Primary, rig.Replica).IsConsistent);
    }

    [Fact]
    public async Task CreateRental_WithoutTransaction_WhenErrorBetweenOperations_LeavesPartialState()
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Иванов Иван", "375291111111", ExperimentCar,
                simulateError: true, withoutTransaction: true));

        Assert.False(response.Success);
        Assert.Contains("Транзакция не использовалась", response.Message);
        Assert.Equal(1, rig.Primary.Database.CountCustomers());
        Assert.Equal(1, rig.Primary.Database.CountRentals());
        Assert.True(rig.Primary.Database.IsAvailable(ExperimentCar));

        // Частичное состояние реплицируется как есть: вторая копия повторяет ошибку.
        Assert.Equal(1, rig.Replica.Database.CountRentals());
        Assert.True(rig.Replica.Database.IsAvailable(ExperimentCar));

        Response second = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Петров Пётр", "375292222222", ExperimentCar));

        Assert.True(second.Success);
        Assert.Equal(2, rig.Primary.Database.CountRentals());
        Assert.Equal(2, rig.Replica.Database.CountRentals());
    }

    [Fact]
    public async Task CreateRental_WithTransaction_WhenErrorBetweenOperations_RollsBack()
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Иванов Иван", "375291111111", ExperimentCar, simulateError: true));

        Assert.False(response.Success);
        Assert.Contains("ROLLBACK", response.Message);
        Assert.Equal(0, rig.Primary.Database.CountCustomers());
        Assert.Equal(0, rig.Primary.Database.CountRentals());
        Assert.True(rig.Primary.Database.IsAvailable(ExperimentCar));
        Assert.True(rig.Replicator.Compare(rig.Primary, rig.Replica).IsConsistent);

        Response retry = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Иванов Иван", "375291111111", ExperimentCar));

        Assert.True(retry.Success);
        Assert.Equal(1, rig.Replica.Database.CountRentals());
    }

    [Fact]
    public async Task TwoClients_SameCar_OnlyOneSucceeds()
    {
        var rig = new TestRig(processingDelayMs: 200);

        Task<Response> first = rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", ExperimentCar, clientName: "Client 1"), clientId: 1);
        Task<Response> second = rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 2", "375291000002", ExperimentCar, clientName: "Client 2"), clientId: 2);

        Response[] responses = await Task.WhenAll(first, second);

        Assert.Equal(1, responses.Count(item => item.Success));
        Assert.Equal(1, responses.Count(item => !item.Success));
        Assert.Contains(responses, item => item.Message == "Автомобиль уже арендован");
        Assert.Equal(1, rig.Primary.Database.CountRentals());
        Assert.Equal(1, rig.Replica.Database.CountRentals());
    }

    [Fact]
    public async Task TwoClients_DifferentCars_BothSucceedWithoutSerialDelay()
    {
        var rig = new TestRig(processingDelayMs: 400);

        var stopwatch = Stopwatch.StartNew();
        Task<Response> first = rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", ExperimentCar), clientId: 1);
        Task<Response> second = rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 2", "375291000002", TestRig.SecondCar), clientId: 2);
        Response[] responses = await Task.WhenAll(first, second);
        stopwatch.Stop();

        Assert.All(responses, response => Assert.True(response.Success));
        Assert.Equal(2, rig.Primary.Database.CountRentals());
        Assert.True(stopwatch.ElapsedMilliseconds < 700,
            $"Запросы заняли {stopwatch.ElapsedMilliseconds} мс: задержка, похоже, попала внутрь lock.");
    }

    [Fact]
    public async Task CreateRental_WhenCarAlreadyRented_ReturnsFail()
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", "4567 DE-1"));

        Assert.False(response.Success);
        Assert.Equal("Автомобиль уже арендован", response.Message);
        Assert.Equal(0, rig.Primary.Database.CountRentals());
    }

    [Fact]
    public async Task CreateRental_WhenCarMissing_ReturnsError()
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", "0000 XX-0"));

        Assert.False(response.Success);
        Assert.Equal("Автомобиль не найден", response.Message);
        Assert.Equal(0, rig.Primary.Database.CountRentals());
    }

    [Fact]
    public async Task CreateRental_WithInvalidData_ReturnsError()
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(new Request
        {
            Operation = "CreateRental",
            Data = JsonSerializer.SerializeToElement(new
            {
                fullName = "",
                phone = "",
                registrationNumber = "",
                startDate = new DateTime(2026, 10, 11),
                days = 0
            })
        });

        Assert.False(response.Success);
        Assert.Equal("Данные для создания аренды указаны некорректно", response.Message);
        Assert.Equal(0, rig.Primary.Database.CountRentals());
    }

    [Theory]
    [InlineData(999_999_999, "2026-10-11")]
    [InlineData(5, "9999-12-30")]
    public async Task CreateRental_WhenEndDateOverflowsDateTime_ReturnsInvalidDataError(int days, string startDateString)
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(new Request
        {
            Operation = "CreateRental",
            Data = JsonSerializer.SerializeToElement(new
            {
                fullName = "Клиент 1",
                phone = "375291000001",
                registrationNumber = ExperimentCar,
                startDate = DateTime.Parse(startDateString),
                days,
                simulateError = false
            })
        });

        Assert.False(response.Success);
        Assert.Equal("Данные для создания аренды указаны некорректно", response.Message);
        Assert.Equal(0, rig.Primary.Database.CountRentals());
        Assert.Equal(0, rig.Primary.Database.CountCustomers());
        Assert.True(rig.Primary.Database.IsAvailable(ExperimentCar));
    }

    [Fact]
    public async Task Handle_WithUnknownOperation_ReturnsError()
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(new Request
        {
            Operation = "UnknownOperation",
            Data = JsonSerializer.SerializeToElement(new { })
        });

        Assert.False(response.Success);
        Assert.Equal("Неизвестная операция", response.Message);
    }

    [Fact]
    public async Task CancelRental_MissingRental_ReturnsError()
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(new Request
        {
            Operation = "CancelRental",
            Data = JsonSerializer.SerializeToElement(new { rentalId = 99 })
        });

        Assert.False(response.Success);
        Assert.Equal("Аренда не найдена", response.Message);
    }

    [Fact]
    public async Task CancelRental_AfterCommit_ReplicatesStatusAndAvailability()
    {
        var rig = new TestRig();
        Response created = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", ExperimentCar));
        int rentalId = created.Data!.Value.GetProperty("id").GetInt32();

        Response cancelled = await rig.Handler.HandleAsync(new Request
        {
            Operation = "CancelRental",
            Data = JsonSerializer.SerializeToElement(new { rentalId })
        });

        Assert.True(cancelled.Success);
        Assert.Equal("Primary", cancelled.Instance);
        Assert.True(rig.Replica.Database.IsAvailable(ExperimentCar));
        Assert.Equal("Cancelled", rig.Replica.Database.GetRental(rentalId)!.Status);
    }

    [Fact]
    public async Task ReturnCar_AfterCommit_MakesCarAvailableAgain()
    {
        var rig = new TestRig();
        Response created = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", ExperimentCar));
        int rentalId = created.Data!.Value.GetProperty("id").GetInt32();

        Response returned = await rig.Handler.HandleAsync(new Request
        {
            Operation = "ReturnCar",
            Data = JsonSerializer.SerializeToElement(new { rentalId })
        });

        Assert.True(returned.Success);
        Assert.True(rig.Primary.Database.IsAvailable(ExperimentCar));
        Assert.Equal("Returned", rig.Primary.Database.GetRental(rentalId)!.Status);
    }

    [Fact]
    public async Task CreateRental_WhenAmountViolatesDatabaseConstraint_RollsBack()
    {
        var rig = new TestRig();

        Response response = await rig.Handler.HandleAsync(
            TestRig.CreateRental("Клиент 1", "375291000001", ExperimentCar, days: 1_000_000));

        Assert.False(response.Success);
        Assert.StartsWith("Ошибка базы данных", response.Message);
        Assert.Equal(0, rig.Primary.Database.CountRentals());
        Assert.Equal(0, rig.Replica.Database.CountRentals());
        Assert.True(rig.Primary.Database.IsAvailable(ExperimentCar));
        Assert.True(rig.Replicator.Compare(rig.Primary, rig.Replica).IsConsistent);
    }

    [Fact]
    public void Database_RejectsUnknownRentalStatus()
    {
        var rig = new TestRig();

        using var connection = rig.Primary.Database.Open();
        int customerId = rig.Primary.Database.GetOrCreateCustomer(connection, null, "Клиент", "375290000000");
        int carId = rig.Primary.Database.GetCarByRegistration(ExperimentCar)!.Id;

        using var command = new NpgsqlCommand(
            """
            INSERT INTO rentals (customer_id, car_id, start_date, end_date, status, amount)
            VALUES (@customerId, @carId, CURRENT_DATE, CURRENT_DATE, 'Bad', 10)
            """,
            connection);
        command.Parameters.AddWithValue("customerId", customerId);
        command.Parameters.AddWithValue("carId", carId);

        PostgresException exception = Assert.Throws<PostgresException>(() => command.ExecuteNonQuery());
        Assert.Equal("23514", exception.SqlState);
        Assert.Equal(0, rig.Primary.Database.CountRentals());
    }

    [Fact]
    public async Task ConnectToUnavailableServer_ThrowsSocketException()
    {
        using var client = new TcpClient();
        await Assert.ThrowsAnyAsync<SocketException>(() => client.ConnectAsync("127.0.0.1", 5998));
    }
}
