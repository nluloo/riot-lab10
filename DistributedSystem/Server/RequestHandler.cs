using System.Text.Json;
using Npgsql;
using Shared.Cluster;
using Shared.Events;
using Shared.Models;
using Shared.Protocol;

namespace Server;

public class RequestHandler
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private static readonly JsonSerializerOptions CamelCase = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    /// Таблицы, которые затрагивает каждая операция изменения. Именно они передаются в Replica.
    private static readonly string[] RentalChangeTables = { "customers", "rentals", "cars" };
    private static readonly string[] StatusChangeTables = { "rentals", "cars" };

    private const string BothUnavailableMessage =
        "Недоступны оба экземпляра данных. Запрос не выполнен, сервер продолжает работу.";

    private readonly InstanceManager _instances;
    private readonly Replicator _replicator;
    private readonly Journal _journal;
    private readonly Publisher? _publisher;
    private readonly Cluster? _cluster;
    private readonly int _processingDelayMs;
    private readonly object _syncLock = new();

    public RequestHandler(
        InstanceManager instances,
        Replicator replicator,
        Journal journal,
        int processingDelayMs = 0,
        Publisher? publisher = null,
        Cluster? cluster = null)
    {
        _instances = instances;
        _replicator = replicator;
        _journal = journal;
        _processingDelayMs = processingDelayMs;
        _publisher = publisher;
        _cluster = cluster;
    }

    public async Task<Response> HandleAsync(Request request, int clientId = 0)
    {
        try
        {
            return request.Operation switch
            {
                "SearchCar" => SearchCar(request, clientId),
                "CreateRental" => await CreateRentalAsync(request, clientId, useTransaction: true),
                "CreateRentalNoTransaction" => await CreateRentalAsync(request, clientId, useTransaction: false),
                "CancelRental" => ChangeRental(request, clientId, "Cancelled", "Аренда отменена", "Эту аренду нельзя отменить"),
                "ReturnCar" => ChangeRental(request, clientId, "Returned", "Автомобиль возвращён", "Этот автомобиль уже возвращён или аренда отменена"),
                "ShowState" => ShowState(clientId),
                "ResetData" => ResetData(clientId),
                "ShowInstances" => ShowInstances(),
                "CompareData" => CompareData(clientId),
                "SyncData" => SyncData(clientId),
                "ReplicationLog" => ReplicationLog(),
                "BrokerStatus" => BrokerStatus(),
                "ClusterStatus" => await ClusterStatusAsync(),
                "ClusterCheck" => await ClusterCheckAsync(),
                "ClusterPropose" => await ClusterProposeAsync(request),
                "ClusterFailNode" => await ClusterControlAsync(request, fail: true),
                "ClusterRestoreNode" => await ClusterControlAsync(request, fail: false),
                "ClusterExperiments" => await ClusterExperimentsAsync(),
                _ => new Response { Success = false, Message = "Неизвестная операция" }
            };
        }
        catch (PostgresException ex)
        {
            return DatabaseError(ex);
        }
        catch (NpgsqlException)
        {
            return new Response { Success = false, Message = "Соединение с экземпляром данных отсутствует" };
        }
    }

    /// <summary>
    /// Сервер — Publisher: формирует событие о уже совершившемся факте и передаёт его Broker'у.
    /// Возвращает строку о судьбе события для ответа клиенту; null — если Publisher не настроен.
    /// Отказ Broker не отменяет операцию: данные уже сохранены и отреплицированы.
    /// </summary>
    private string? Publish(string clientLabel, int clientId, string eventType, int entityId, Dictionary<string, object?> data)
    {
        if (_publisher == null)
        {
            _journal.Write("Publisher", $"{eventType} #{entityId} НЕ ОПУБЛИКОВАН: сервер запущен без Broker (PUBLISHER_BROKERS не задан)");
            return null;
        }

        var evt = new DomainEvent
        {
            EventType = eventType,
            EntityId = entityId,
            ClientId = clientId,
            Data = data
        };

        List<PublishResultMessage> results = _publisher.Publish(evt);

        _journal.Write(clientLabel, $"Event {eventType} #{entityId}: {string.Join(" | ", results.Select(result => $"{result.BrokerId} {result.Status}"))}");

        return Publisher.Describe(evt, results);
    }

    private Response BrokerStatus()
    {
        if (_publisher == null)
        {
            return new Response
            {
                Success = false,
                Message = "Сервер запущен без Publisher: Pub/Sub отключён, операции работают как в ЛР №5."
            };
        }

        List<BrokerInfoResultMessage> brokers = _publisher.GetBrokersInfo();
        var lines = new List<string>();

        foreach (BrokerInfoResultMessage broker in brokers)
        {
            lines.Add($"{broker.BrokerId}: {(broker.Success ? broker.Status : broker.Status + " — " + broker.Message)}, " +
                      $"подписчиков на связи {broker.OnlineSubscribers}, событий в очереди {broker.QueuedEvents}");

            lines.AddRange(broker.Subscriptions.Select(subscription => $"  подписка: {subscription}"));
            lines.AddRange(broker.Statuses.Select(status => $"  состояние: {status}"));
        }

        return new Response
        {
            Success = brokers.Any(broker => broker.Success),
            Message = $"Broker: {brokers.Count} шт., доступно {brokers.Count(broker => broker.Success)}.",
            Data = JsonSerializer.SerializeToElement(lines, CamelCase)
        };
    }

    /// <summary>
    /// Узлы кластера серверу не нужны для работы: если они не отвечают, операция выполняется как в ЛР №6.
    /// Но если узлы живые и большинства нет — решение не принимается, данные не меняются.
    /// </summary>
    private const string ClusterOffMessage =
        "Сервер запущен без кластера: CLUSTER_MEMBERS пуст, операции работают как в ЛР №6.";

    private async Task<Response> ClusterStatusAsync()
    {
        if (_cluster == null || !_cluster.Enabled)
        {
            return new Response { Success = false, Message = ClusterOffMessage };
        }

        List<string> lines = await _cluster.StatusLinesAsync();

        return new Response
        {
            Success = true,
            Message = $"Узлов {_cluster.TotalNodes}, большинство {_cluster.Majority}.",
            Data = JsonSerializer.SerializeToElement(lines, CamelCase)
        };
    }

    private async Task<Response> ClusterCheckAsync()
    {
        if (_cluster == null || !_cluster.Enabled)
        {
            return new Response { Success = false, Message = ClusterOffMessage };
        }

        ClusterInfoMessage? info = await _cluster.CheckAsync();

        if (info == null)
        {
            return new Response
            {
                Success = false,
                Message = "Ни один узел не ответил. Кластер не запущен: поднимите пять процессов Node или контейнеры node1…node5."
            };
        }

        List<string> lines = info.Nodes.Select(DescribeView).ToList();

        return new Response
        {
            Success = info.LeaderId != 0,
            Message = info.LeaderId == 0
                ? $"Лидера нет: доступно {info.Nodes.Count(node => node.Status == ClusterStates.Available)} узлов " +
                  $"из {info.TotalNodes}, нужно {info.Majority}. Решения не принимаются."
                : $"Лидер Node {info.LeaderId}, зафиксировано Version={info.CommittedVersion}.",
            Consensus = $"{LeaderName(info.LeaderId)}, доступно {info.Nodes.Count(node => node.Status == ClusterStates.Available)}/{info.TotalNodes}, большинство {info.Majority}",
            Data = JsonSerializer.SerializeToElement(lines, CamelCase)
        };
    }

    private async Task<Response> ClusterProposeAsync(Request request)
    {
        if (_cluster == null || !_cluster.Enabled)
        {
            return new Response { Success = false, Message = ClusterOffMessage };
        }

        ClusterProposeData? data;

        try
        {
            data = request.Data.Deserialize<ClusterProposeData>(JsonOptions);
        }
        catch (JsonException)
        {
            return new Response { Success = false, Message = "Данные предложения указаны некорректно" };
        }

        if (data == null || string.IsNullOrWhiteSpace(data.Value))
        {
            return new Response { Success = false, Message = "Не указано значение предложения" };
        }

        if (data.Version < 0)
        {
            return new Response { Success = false, Message = "Version не может быть отрицательной" };
        }

        ConsensusResultMessage? result = await _cluster.ProposeAsync(
            data.Value.Trim(),
            data.Version > 0 ? data.Version : null);

        if (result == null)
        {
            return new Response
            {
                Success = false,
                Message = "Узлы кластера не ответили: консенсус проверить не на что. Операции предметной области при этом работают."
            };
        }

        List<string> lines = result.Votes.Count > 0
            ? result.Votes
            : new List<string> { result.Message };

        return new Response
        {
            Success = result.Achieved,
            Message = $"{result.Verdict}: Version={result.Version}, {result.Value}",
            Consensus = Describe(result),
            Data = JsonSerializer.SerializeToElement(lines, CamelCase)
        };
    }

    private async Task<Response> ClusterControlAsync(Request request, bool fail)
    {
        if (_cluster == null || !_cluster.Enabled)
        {
            return new Response { Success = false, Message = ClusterOffMessage };
        }

        ClusterNodeData? data;

        try
        {
            data = request.Data.Deserialize<ClusterNodeData>(JsonOptions);
        }
        catch (JsonException)
        {
            return new Response { Success = false, Message = "Данные узла указаны некорректно" };
        }

        if (data == null || data.NodeId <= 0)
        {
            return new Response { Success = false, Message = "Не указан номер узла" };
        }

        if (!_cluster.Members.Any(member => member.Id == data.NodeId))
        {
            return new Response
            {
                Success = false,
                Message = $"Узла Node {data.NodeId} нет в составе кластера: " +
                          $"{string.Join(", ", _cluster.Members.Select(member => $"Node {member.Id}"))}."
            };
        }

        _journal.Write($"Client", $"{(fail ? "отказ" : "восстановление")} узла Node {data.NodeId} запрошен клиентом");

        NodeControlMessage? result = await _cluster.ControlAsync(data.NodeId, fail);

        if (result == null)
        {
            return new Response
            {
                Success = false,
                Message = $"Node {data.NodeId} не принял команду '{(fail ? "fail" : "restore")}' — процесс узла не отвечает."
            };
        }

        // Отказ замечают heartbeat-раундом, чтобы перевыборы были видны сразу, а не через интервал опроса.
        await _cluster.CheckAsync();

        List<string> lines = (await _cluster.ViewsAsync()).Select(DescribeView).ToList();

        return new Response
        {
            Success = true,
            Message = result.Message,
            Consensus = $"доступно {lines.Count} записей, состояние — в списке ниже",
            Data = JsonSerializer.SerializeToElement(lines, CamelCase)
        };
    }

    private async Task<Response> ClusterExperimentsAsync()
    {
        if (_cluster == null || !_cluster.Enabled)
        {
            return new Response { Success = false, Message = ClusterOffMessage };
        }

        List<string> lines = await _cluster.ExperimentsAsync();

        return new Response
        {
            Success = lines.Count == 4,
            Message = $"Прогнано экспериментов: {lines.Count}. Отказ 0, 1, 2 и 3 узлов.",
            Data = JsonSerializer.SerializeToElement(lines, CamelCase)
        };
    }

    private static string DescribeView(NodeViewMessage view)
    {
        string role = view.Status == ClusterStates.Available
            ? (string.IsNullOrEmpty(view.Role) ? ClusterStates.Follower : view.Role)
            : "—";

        return $"Node {view.Id}: {view.Status}, {role}, {view.Endpoint}, Version = {view.CommittedVersion}";
    }

    private static string LeaderName(int leaderId) => leaderId == 0 ? "лидер не выбран" : $"лидер Node {leaderId}";

    private static string Describe(ConsensusResultMessage result) =>
        $"{LeaderName(result.LeaderId)}: YES = {result.Yes}, NO = {result.No}, без ответа = {result.Unanswered}, " +
        $"Majority = {result.Majority} -> {result.Verdict}";

    private Response SearchCar(Request request, int clientId)
    {
        if (request.Data.ValueKind != JsonValueKind.Object)
        {
            return new Response { Success = false, Message = "Параметры поиска указаны некорректно." };
        }

        string clientLabel = ClientLabel(clientId, null);
        Log(clientLabel, "Request received: SearchCar");

        DataInstance? instance = _instances.SelectForRequest(clientLabel);

        if (instance == null)
        {
            Log(clientLabel, "Request -> FAIL");
            return new Response { Success = false, Message = BothUnavailableMessage };
        }

        string? location = GetFilterValue(request.Data, "location");
        string? brandOrModel = GetFilterValue(request.Data, "brandOrModel");
        List<Car> cars = instance.Database.SearchCars(location, brandOrModel);

        Log(clientLabel, $"Read from {instance.Name} -> SUCCESS, {cars.Count} доступных автомобилей");

        return new Response
        {
            Success = true,
            Message = $"Доступных автомобилей: {cars.Count}. Данные читает {instance.Name}.",
            Instance = instance.Name,
            Data = JsonSerializer.SerializeToElement(cars, CamelCase)
        };
    }

    private async Task<Response> CreateRentalAsync(Request request, int clientId, bool useTransaction)
    {
        CreateRentalData? data;

        try
        {
            data = request.Data.Deserialize<CreateRentalData>(JsonOptions);
        }
        catch (JsonException)
        {
            return new Response { Success = false, Message = "Данные для создания аренды указаны некорректно" };
        }

        if (data == null || string.IsNullOrWhiteSpace(data.FullName) ||
            string.IsNullOrWhiteSpace(data.Phone) ||
            string.IsNullOrWhiteSpace(data.RegistrationNumber) ||
            data.Days <= 0 || data.StartDate == default ||
            (DateTime.MaxValue - data.StartDate.Date).TotalDays < data.Days)
        {
            return new Response { Success = false, Message = "Данные для создания аренды указаны некорректно" };
        }

        string clientLabel = ClientLabel(clientId, data.ClientName);
        string operationName = useTransaction ? "CreateRental" : "CreateRentalNoTransaction";
        Log(clientLabel, $"START {operationName}");

        // Кластер согласует решение до изменения данных: без большинства сервер их не трогает.
        ConsensusResultMessage? consensus = await AgreeOnRentalAsync(clientLabel, data);

        if (consensus != null && !consensus.Achieved)
        {
            Log(clientLabel, "FAIL");

            return new Response
            {
                Success = false,
                Consensus = Describe(consensus),
                Message = $"{consensus.Verdict}: YES = {consensus.Yes}, Majority = {consensus.Majority}. " +
                          (string.IsNullOrEmpty(consensus.Message) ? string.Empty : consensus.Message + " ") +
                          "Изменения в базе данных не выполнялись."
            };
        }

        string? consensusLine = consensus == null ? null : Describe(consensus);

        if (_processingDelayMs > 0)
        {
            await Task.Delay(_processingDelayMs);
        }

        lock (_syncLock)
        {
            Log(clientLabel, "LOCK");

            try
            {
                DataInstance? instance = _instances.SelectForRequest(clientLabel);

                if (instance == null)
                {
                    Log(clientLabel, "FAIL");
                    return new Response { Success = false, Message = BothUnavailableMessage };
                }

                Car? car;

                try
                {
                    car = instance.Database.GetCarByRegistration(data.RegistrationNumber);
                }
                catch (NpgsqlException)
                {
                    Log(clientLabel, "FAIL");
                    return new Response { Success = false, Message = $"Соединение с {instance.Name} отсутствует" };
                }

                Log(clientLabel, $"Check resource on {instance.Name}");

                if (car == null)
                {
                    Log(clientLabel, "FAIL");
                    return new Response { Success = false, Message = "Автомобиль не найден" };
                }

                Log(clientLabel, $"Available = {(car.IsAvailable ? 1 : 0)}");

                if (!car.IsAvailable)
                {
                    Log(clientLabel, "FAIL");
                    return new Response { Success = false, Message = "Автомобиль уже арендован" };
                }

                Response outcome = SaveRental(clientLabel, clientId, instance, car, data, useTransaction);
                outcome.Consensus = consensusLine;

                return outcome;
            }
            finally
            {
                Log(clientLabel, "UNLOCK");
            }
        }
    }

    /// Лидер кластера предлагает значение «аренда активна» и собирает голоса.
    /// null возвращается, когда консенсус не требовался: кластер выключён или узлы не отвечают —
    /// тогда сервер выполняет операцию как в ЛР №6 (отказ внешнего сервиса не ломает учёт аренды).
    private async Task<ConsensusResultMessage?> AgreeOnRentalAsync(string clientLabel, CreateRentalData data)
    {
        if (_cluster == null || !_cluster.Enabled)
        {
            return null;
        }

        string value = $"RentalStatus = Active (автомобиль {data.RegistrationNumber.Trim()}, клиент {data.FullName.Trim()})";

        ConsensusResultMessage? result = await _cluster.ProposeAsync(value);

        if (result == null)
        {
            Log(clientLabel, "Consensus -> SKIPPED: узлы кластера не отвечают, операция выполняется без консенсуса");
            return null;
        }

        Log(clientLabel, $"Consensus -> {result.Verdict}: {Describe(result)}");

        return result;
    }

    private Response SaveRental(
        string clientLabel,
        int clientId,
        DataInstance instance,
        Car car,
        CreateRentalData data,
        bool useTransaction)
    {
        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;

        try
        {
            connection = instance.Database.Open();

            if (useTransaction)
            {
                transaction = connection.BeginTransaction();
                Log(clientLabel, "BEGIN");
            }

            int customerId = instance.Database.GetOrCreateCustomer(connection, transaction, data.FullName.Trim(), data.Phone.Trim());
            decimal amount = car.DailyPrice * data.Days;
            DateTime startDate = data.StartDate.Date;
            DateTime endDate = startDate.AddDays(data.Days);
            int rentalId = instance.Database.InsertRental(connection, transaction, customerId, car.Id, startDate, endDate, amount);
            Log(clientLabel, $"Operation 1: INSERT rental SUCCESS на {instance.Name}");

            if (data.SimulateError)
            {
                Log(clientLabel, "ERROR");

                if (transaction == null)
                {
                    // Без транзакции первая операция уже сохранена: её и передаём в вторую копию.
                    FinishChange(clientLabel, instance, new[] { "customers", "rentals" });
                }

                throw new ArtificialTransactionException();
            }

            int updatedCars = instance.Database.SetCarAvailability(connection, transaction, car.Id, false);

            if (updatedCars != 1)
            {
                throw new InvalidOperationException("Попытка изменить отсутствующую запись");
            }

            Log(clientLabel, $"Operation 2: UPDATE car SUCCESS на {instance.Name}");

            if (transaction != null)
            {
                try
                {
                    transaction.Commit();
                }
                catch (Exception)
                {
                    Log(clientLabel, "COMMIT не выполнен");
                    Rollback(clientLabel, transaction);
                    return new Response
                    {
                        Success = false,
                        Message = "Невозможно выполнить COMMIT. Выполнен ROLLBACK."
                    };
                }

                transaction = null;
                Log(clientLabel, "COMMIT");
            }

            FinishChange(clientLabel, instance, RentalChangeTables);

            var rental = new Rental
            {
                Id = rentalId,
                CustomerId = customerId,
                CarId = car.Id,
                StartDate = startDate,
                EndDate = endDate,
                Status = "Active",
                Amount = amount
            };

            Log(clientLabel, $"SUCCESS через {instance.Name}");

            string? events = Publish(clientLabel, clientId, EventTypes.RentalCreated, rentalId, new Dictionary<string, object?>
            {
                ["registrationNumber"] = car.RegistrationNumber,
                ["car"] = $"{car.Brand} {car.Model}",
                ["customer"] = data.FullName,
                ["amount"] = amount,
                ["days"] = data.Days
            });

            string message = useTransaction
                ? $"Аренда создана. Выполнен COMMIT в {instance.Name}. Номер аренды: {rentalId}."
                : $"Аренда создана без транзакции. Номер аренды: {rentalId}. Обе операции сохранены по отдельности в {instance.Name}.";

            return new Response
            {
                Success = true,
                Message = message,
                Instance = instance.Name,
                Events = events,
                Data = JsonSerializer.SerializeToElement(rental, CamelCase)
            };
        }
        catch (ArtificialTransactionException)
        {
            if (transaction != null)
            {
                Rollback(clientLabel, transaction);
                Log(clientLabel, "ROLLBACK: данные не изменились, репликация не нужна");
                Log(clientLabel, "Replication -> NOT NEEDED");

                return new Response
                {
                    Success = false,
                    Instance = instance.Name,
                    Message = "Ошибка внутри транзакции. Выполнен ROLLBACK. Изменения отменены в обеих копиях."
                };
            }

            Log(clientLabel, "FAIL");
            return new Response
            {
                Success = false,
                Instance = instance.Name,
                Message = "Ошибка между операциями. Транзакция не использовалась: аренда сохранена, " +
                          "состояние автомобиля не изменено. Изменённые таблицы переданы в резервную копию."
            };
        }
        catch (Exception ex) when (ex is PostgresException or NpgsqlException or InvalidOperationException)
        {
            if (transaction != null)
            {
                Rollback(clientLabel, transaction);
            }

            Log(clientLabel, "FAIL");
            return DatabaseError(ex);
        }
        finally
        {
            transaction?.Dispose();
            connection?.Dispose();
        }
    }

    private Response ChangeRental(
        Request request,
        int clientId,
        string newStatus,
        string successMessage,
        string invalidStatusMessage)
    {
        RentalIdData? data;

        try
        {
            data = request.Data.Deserialize<RentalIdData>(JsonOptions);
        }
        catch (JsonException)
        {
            return new Response { Success = false, Message = "Некорректные данные запроса" };
        }

        if (data == null || data.RentalId <= 0)
        {
            return new Response { Success = false, Message = "Некорректные данные запроса" };
        }

        string clientLabel = ClientLabel(clientId, null);
        string operation = newStatus == "Cancelled" ? "CancelRental" : "ReturnCar";
        Log(clientLabel, $"START {operation}");

        lock (_syncLock)
        {
            Log(clientLabel, "LOCK");

            try
            {
                DataInstance? instance = _instances.SelectForRequest(clientLabel);

                if (instance == null)
                {
                    Log(clientLabel, "FAIL");
                    return new Response { Success = false, Message = BothUnavailableMessage };
                }

                Rental? rental;

                try
                {
                    rental = instance.Database.GetRental(data.RentalId);
                }
                catch (NpgsqlException)
                {
                    Log(clientLabel, "FAIL");
                    return new Response { Success = false, Message = $"Соединение с {instance.Name} отсутствует" };
                }

                if (rental == null)
                {
                    Log(clientLabel, "FAIL");
                    return new Response { Success = false, Message = "Аренда не найдена" };
                }

                if (rental.Status != "Active")
                {
                    Log(clientLabel, "FAIL");
                    return new Response { Success = false, Message = invalidStatusMessage };
                }

                return SaveRentalStatus(clientLabel, clientId, instance, rental, newStatus, successMessage);
            }
            finally
            {
                Log(clientLabel, "UNLOCK");
            }
        }
    }

    private Response SaveRentalStatus(
        string clientLabel,
        int clientId,
        DataInstance instance,
        Rental rental,
        string newStatus,
        string successMessage)
    {
        using NpgsqlConnection connection = instance.Database.Open();
        NpgsqlTransaction? transaction = connection.BeginTransaction();

        try
        {
            Log(clientLabel, "BEGIN");

            int updatedRentals = instance.Database.UpdateRentalStatus(connection, transaction, rental.Id, newStatus);

            if (updatedRentals != 1)
            {
                throw new InvalidOperationException("Попытка изменить отсутствующую запись");
            }

            int updatedCars = instance.Database.SetCarAvailability(connection, transaction, rental.CarId, true);

            if (updatedCars != 1)
            {
                throw new InvalidOperationException("Попытка изменить отсутствующую запись");
            }

            try
            {
                transaction.Commit();
            }
            catch (Exception)
            {
                Log(clientLabel, "COMMIT не выполнен");
                Rollback(clientLabel, transaction);
                return new Response
                {
                    Success = false,
                    Instance = instance.Name,
                    Message = "Невозможно выполнить COMMIT. Выполнен ROLLBACK."
                };
            }

            transaction = null;
            Log(clientLabel, "COMMIT");

            FinishChange(clientLabel, instance, StatusChangeTables);

            string eventType = newStatus == "Cancelled" ? EventTypes.RentalCancelled : EventTypes.CarReturned;

            string? events = Publish(clientLabel, clientId, eventType, rental.Id, new Dictionary<string, object?>
            {
                ["carId"] = rental.CarId,
                ["status"] = newStatus,
                ["amount"] = rental.Amount
            });

            Log(clientLabel, $"SUCCESS через {instance.Name}");
            return new Response
            {
                Success = true,
                Message = successMessage,
                Instance = instance.Name,
                Events = events
            };
        }
        catch (Exception ex) when (ex is PostgresException or NpgsqlException or InvalidOperationException)
        {
            if (transaction != null)
            {
                Rollback(clientLabel, transaction);
            }

            Log(clientLabel, "FAIL");
            return DatabaseError(ex);
        }
        finally
        {
            transaction?.Dispose();
            connection.Dispose();
        }
    }

    private Response ShowState(int clientId)
    {
        string clientLabel = ClientLabel(clientId, null);
        Log(clientLabel, "Request received: ShowState");

        DataInstance? instance = _instances.SelectForRequest(clientLabel);

        if (instance == null)
        {
            Log(clientLabel, "Request -> FAIL");
            return new Response { Success = false, Message = BothUnavailableMessage };
        }

        var state = new
        {
            Instance = instance.Name,
            Lsn = instance.Lsn,
            Cars = instance.Database.GetCars(),
            Rentals = instance.Database.GetRentals()
        };

        Log(clientLabel, $"Read from {instance.Name} -> SUCCESS");

        return new Response
        {
            Success = true,
            Message = $"Автомобилей: {state.Cars.Count}. Аренд: {state.Rentals.Count}. Читает {instance.Name} (lsn = {state.Lsn}).",
            Instance = instance.Name,
            Data = JsonSerializer.SerializeToElement(state, CamelCase)
        };
    }

    private Response ResetData(int clientId)
    {
        string clientLabel = ClientLabel(clientId, null);
        Log(clientLabel, "START ResetData");

        lock (_syncLock)
        {
            DataInstance? instance = _instances.SelectForRequest(clientLabel);

            if (instance == null)
            {
                Log(clientLabel, "FAIL");
                return new Response { Success = false, Message = BothUnavailableMessage };
            }

            long lsn = instance.Database.Reset();
            instance.RememberLsn(lsn);
            Log(clientLabel, $"{instance.Name} -> RESET, lsn = {lsn}");

            DataInstance peer = _instances.Peer(instance);
            string message = $"Данные сброшены в {instance.Name}. Автомобиль 1234 AB-7 снова доступен (Available = 1).";

            if (peer.Refresh())
            {
                _replicator.FullSync(instance, peer, clientLabel);
                message += $" {peer.Name} синхронизирована.";
            }
            else
            {
                Log(clientLabel, $"Replication -> SKIPPED, {peer.Name} -> UNAVAILABLE");
                message += $" {peer.Name} недоступна, синхронизация отложена.";
            }

            Log(clientLabel, "SUCCESS");

            return new Response { Success = true, Message = message, Instance = instance.Name };
        }
    }

    private Response ShowInstances()
    {
        _instances.RefreshStates();

        InstanceInfo[] info =
        {
            Describe(_instances.Primary),
            Describe(_instances.Replica)
        };

        string summary = string.Join("; ", info.Select(item => $"{item.Name} -> {item.State} (lsn = {item.Lsn})"));

        return new Response
        {
            Success = true,
            Message = summary,
            Instance = _instances.Active?.Name,
            Data = JsonSerializer.SerializeToElement(new { Active = _instances.Active?.Name, Instances = info }, CamelCase)
        };
    }

    private Response CompareData(int clientId)
    {
        string clientLabel = ClientLabel(clientId, null);
        Log(clientLabel, "Request received: CompareData");

        _instances.RefreshStates();

        if (!_instances.Primary.IsAvailable || !_instances.Replica.IsAvailable)
        {
            string down = !_instances.Primary.IsAvailable ? _instances.Primary.Name : _instances.Replica.Name;
            Log(clientLabel, $"Compare -> FAIL, {down} недоступна");

            return new Response
            {
                Success = false,
                Message = $"Сравнение невозможно: {down} недоступна. Проверьте состояние экземпляров."
            };
        }

        DiffReport report = _replicator.Compare(_instances.Primary, _instances.Replica);
        Log(clientLabel, report.IsConsistent
            ? "Compare -> Primary = Replica"
            : $"Compare -> РАСХОЖДЕНИЕ, {report.Differences.Count} отличий");

        var data = new
        {
            report.PrimaryLsn,
            report.ReplicaLsn,
            report.Differences,
            report.DataMatch,
            report.IsConsistent
        };

        return new Response
        {
            Success = true,
            Message = report.IsConsistent
                ? "Копии согласованы: Primary = Replica."
                : $"Копии различаются ({report.Differences.Count} отличий). Актуальные данные: " +
                  $"{(report.ReplicaLsn > report.PrimaryLsn ? _instances.Replica.Name : _instances.Primary.Name)}.",
            Data = JsonSerializer.SerializeToElement(data, CamelCase)
        };
    }

    private Response SyncData(int clientId)
    {
        string clientLabel = ClientLabel(clientId, null);
        Log(clientLabel, "START SyncData");

        lock (_syncLock)
        {
            _instances.RefreshStates();

            if (!_instances.Primary.IsAvailable || !_instances.Replica.IsAvailable)
            {
                string down = !_instances.Primary.IsAvailable ? _instances.Primary.Name : _instances.Replica.Name;
                Log(clientLabel, $"Synchronization -> FAIL, {down} недоступна");

                return new Response
                {
                    Success = false,
                    Message = $"Синхронизация невозможна: {down} недоступна. Поднимите экземпляр и повторите."
                };
            }

            DiffReport before = _replicator.Compare(_instances.Primary, _instances.Replica);

            if (before.IsConsistent)
            {
                Log(clientLabel, "Synchronization -> NOT NEEDED");
                return new Response
                {
                    Success = true,
                    Message = "Копии уже согласованы, синхронизация не требуется."
                };
            }

            DataInstance source = ChooseSource(before);
            DataInstance target = _instances.Peer(source);

            Log(clientLabel, $"Synchronization -> START, {source.Name} -> {target.Name}");
            _replicator.FullSync(source, target, clientLabel);

            DiffReport after = _replicator.Compare(_instances.Primary, _instances.Replica);
            Log(clientLabel, after.IsConsistent ? "Compare -> Primary = Replica" : "Compare -> РАСХОЖДЕНИЕ осталось");

            string message = after.IsConsistent
                ? $"Синхронизация выполнена: {target.Name} обновлена из {source.Name}, Primary = Replica."
                : $"Синхронизация не привела к согласованности: осталось {after.Differences.Count} отличий.";

            return new Response
            {
                Success = after.IsConsistent,
                Message = message,
                Data = JsonSerializer.SerializeToElement(new { source = source.Name, target = target.Name, after.Differences }, CamelCase)
            };
        }
    }

    private Response ReplicationLog()
    {
        List<string> entries = _journal.Recent(40);

        return new Response
        {
            Success = true,
            Message = $"Последние {entries.Count} событий журнала сервера.",
            Data = JsonSerializer.SerializeToElement(entries, CamelCase)
        };
    }

    /// Изменение в активной копии выполнено: увеличиваем версию и передаём затронутые таблицы
    /// во вторую копию. Если вторая копия недоступна, изменения остаются непереданными —
    /// расхождение видно по lsn и устраняется при восстановлении.
    private void FinishChange(string clientLabel, DataInstance instance, string[] tables)
    {
        long lsn = instance.Database.BumpLsn();
        instance.RememberLsn(lsn);
        Log(clientLabel, $"{instance.Name} -> UPDATE, lsn = {lsn}");

        DataInstance peer = _instances.Peer(instance);

        Log(clientLabel, "Replication -> START");

        if (!peer.Refresh())
        {
            Log(clientLabel, $"Replication -> SKIPPED, {peer.Name} -> UNAVAILABLE");
            return;
        }

        try
        {
            _replicator.Replicate(instance, peer, tables, clientLabel);
        }
        catch (Exception ex) when (ex is PostgresException or NpgsqlException or InvalidOperationException)
        {
            Log(clientLabel, $"Replication -> FAIL: {ShortMessage(ex)}");
            peer.Refresh();
        }
    }

    private DataInstance ChooseSource(DiffReport report)
    {
        if (report.ReplicaLsn > report.PrimaryLsn)
        {
            return _instances.Replica;
        }

        if (report.PrimaryLsn > report.ReplicaLsn)
        {
            return _instances.Primary;
        }

        // Версии равны, а данные различаются: источником считаем копию, которая обслуживала запросы.
        return _instances.Active ?? _instances.Primary;
    }

    private InstanceInfo Describe(DataInstance instance)
    {
        var info = new InstanceInfo
        {
            Name = instance.Name,
            State = instance.StatusText,
            IsActive = ReferenceEquals(instance, _instances.Active)
        };

        if (!instance.IsAvailable)
        {
            // Данные недоступны: показываем последнюю известную версию, значения прочитать нельзя.
            info.Lsn = instance.Lsn;
            return info;
        }

        info.Lsn = instance.Database.GetLsn();
        info.Cars = instance.Database.GetCars();
        info.Rentals = instance.Database.CountRentals();

        return info;
    }

    private void Rollback(string clientLabel, NpgsqlTransaction transaction)
    {
        try
        {
            transaction.Rollback();
            Log(clientLabel, "ROLLBACK");
        }
        catch (Exception)
        {
            Log(clientLabel, "ROLLBACK не выполнен");
        }
    }

    private static Response DatabaseError(Exception exception)
    {
        if (exception is PostgresException postgres)
        {
            if (postgres.SqlState is "23505" or "23503" or "23514")
            {
                return new Response { Success = false, Message = "Нарушение ограничения базы данных" };
            }

            return new Response { Success = false, Message = "Ошибка базы данных: " + postgres.MessageText };
        }

        if (exception is NpgsqlException)
        {
            return new Response { Success = false, Message = "Соединение с экземпляром данных отсутствует" };
        }

        return new Response { Success = false, Message = exception.Message };
    }

    private static string ShortMessage(Exception exception) =>
        exception.Message.Split('\n')[0].Trim();

    private static string? GetFilterValue(JsonElement data, string propertyName)
    {
        if (!data.TryGetProperty(propertyName, out JsonElement value) ||
            value.ValueKind != JsonValueKind.String)
        {
            return null;
        }

        return value.GetString();
    }

    private static string ClientLabel(int clientId, string? clientName)
    {
        if (string.IsNullOrWhiteSpace(clientName))
        {
            return $"Client {clientId}";
        }

        string cleaned = clientName.Replace("\r", string.Empty).Replace("\n", string.Empty).Trim();
        return cleaned.Length == 0 ? $"Client {clientId}" : cleaned;
    }

    private void Log(string clientLabel, string message) => _journal.Write(clientLabel, message);
}

public class ArtificialTransactionException : Exception
{
    public ArtificialTransactionException()
        : base("Искусственная ошибка между операциями")
    {
    }
}
