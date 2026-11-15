using System.Net.Sockets;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Shared.Protocol;

Console.OutputEncoding = Encoding.UTF8;

// Значения по умолчанию совпадают с константами ЛР №7, поэтому запуск на хосте не меняется.
string? serverHostSetting = Environment.GetEnvironmentVariable("SERVER_HOST");
string serverAddress = string.IsNullOrWhiteSpace(serverHostSetting) ? "127.0.0.1" : serverHostSetting;
int serverPort = ReadIntEnvironment("SERVER_PORT", 5000);
const string experimentCar = "1234 AB-7";
string[] experimentCars = { "1234 AB-7", "2345 BC-7", "3456 CD-1" };

Console.WriteLine($"Сервер: {serverAddress}:{serverPort}");
Console.WriteLine("Клиент не видит ни сервер, ни оба экземпляра данных: запросы идут только на сервер.");

while (true)
{
    Console.WriteLine("\nАренда автомобилей (ЛР №8) — выборы лидера и консенсус поверх Pub/Sub");
    Console.WriteLine("1  - Найти автомобиль");
    Console.WriteLine("2  - Создать аренду (с транзакцией)");
    Console.WriteLine("3  - Отменить аренду");
    Console.WriteLine("4  - Вернуть автомобиль");
    Console.WriteLine("5  - Показать данные из активного экземпляра");
    Console.WriteLine("6  - Без транзакции, ошибка между операциями");
    Console.WriteLine("7  - С транзакцией, COMMIT");
    Console.WriteLine("8  - С транзакцией, ошибка и ROLLBACK");
    Console.WriteLine("9  - Сбросить данные в обеих копиях");
    Console.WriteLine("10 - Состояние экземпляров (Primary, Replica)");
    Console.WriteLine("11 - Сравнить копии");
    Console.WriteLine("12 - Синхронизировать копии");
    Console.WriteLine("13 - Журнал репликации");
    Console.WriteLine("14 - Эксперимент: три изменения подряд и сверка копий");
    Console.WriteLine("15 - Состояние Broker: подписки, кто на связи, очередь");
    Console.WriteLine("16 - Состояние кластера: узлы, роли, лидер, большинство");
    Console.WriteLine("17 - Heartbeat-проверка: обнаружить отказ и выбрать лидера");
    Console.WriteLine("18 - Предложить значение кластеру (проверка консенсуса)");
    Console.WriteLine("19 - Отметить узел FAILED (отказ)");
    Console.WriteLine("20 - Восстановить узел (AVAILABLE)");
    Console.WriteLine("21 - Эксперименты 1-4: отказ 0, 1, 2, 3 узлов и решение");
    Console.WriteLine("0  - Выход");
    Console.Write("Выберите операцию: ");

    string? choice = Console.ReadLine();

    if (choice == null || choice == "0")
    {
        break;
    }

    switch (choice)
    {
        case "6":
            await RunTransactionExperimentAsync(useTransaction: false, simulateError: true);
            continue;
        case "7":
            await RunTransactionExperimentAsync(useTransaction: true, simulateError: false);
            continue;
        case "8":
            await RunTransactionExperimentAsync(useTransaction: true, simulateError: true);
            continue;
        case "9":
            await SendRequestAsync(EmptyRequest("ResetData"));
            continue;
        case "10":
            await ShowInstancesAsync();
            continue;
        case "11":
            await CompareDataAsync();
            continue;
        case "12":
            await SyncDataAsync();
            continue;
        case "13":
            await ReplicationLogAsync();
            continue;
        case "14":
            await RunReplicationExperimentAsync();
            continue;
        case "15":
            await BrokerStatusAsync();
            continue;
        case "16":
            await PrintLinesAsync(EmptyRequest("ClusterStatus"), "Узлы кластера");
            continue;
        case "17":
            await ClusterCheckAsync();
            continue;
        case "18":
            await ClusterProposeAsync();
            continue;
        case "19":
            await ClusterNodeAsync("ClusterFailNode");
            continue;
        case "20":
            await ClusterNodeAsync("ClusterRestoreNode");
            continue;
        case "21":
            await PrintLinesAsync(EmptyRequest("ClusterExperiments"), "Эксперименты 1-4 (отказ 0, 1, 2, 3 узлов)");
            continue;
    }

    string? operation = choice switch
    {
        "1" => "SearchCar",
        "2" => "CreateRental",
        "3" => "CancelRental",
        "4" => "ReturnCar",
        "5" => "ShowState",
        _ => null
    };

    if (operation == null)
    {
        Console.WriteLine("Неизвестный пункт меню");
        continue;
    }

    object? requestData = GetRequestData(operation);

    if (requestData == null)
    {
        continue;
    }

    Response? response = await SendRequestAsync(new Request
    {
        Operation = operation,
        Data = JsonSerializer.SerializeToElement(requestData)
    });

    if (operation == "ShowState")
    {
        PrintState(response);
    }
}

async Task RunTransactionExperimentAsync(bool useTransaction, bool simulateError)
{
    string mode = useTransaction ? "с транзакцией" : "без транзакции";
    string error = simulateError ? "с ошибкой между операциями" : "без ошибки";
    Console.WriteLine($"\n=== Составная операция {mode}, {error} ===");
    Console.WriteLine("Операция 1: INSERT rental. Операция 2: UPDATE car (is_available = false).");
    Console.WriteLine("Журнал BEGIN/COMMIT/ROLLBACK и Replication — в консоли сервера и в пункте 13.\n");

    if (!await ResetAsync())
    {
        return;
    }

    var request = new Request
    {
        Operation = useTransaction ? "CreateRental" : "CreateRentalNoTransaction",
        Data = JsonSerializer.SerializeToElement(new
        {
            fullName = "Иванов Иван",
            phone = "375291000099",
            registrationNumber = experimentCar,
            startDate = DateTime.UtcNow.Date,
            days = 2,
            simulateError,
            clientName = "Client 1"
        })
    };

    await SendRequestAsync(request, "Client 1");
    Console.WriteLine("\nСостояние базы после эксперимента:");
    PrintState(await ShowStateAsync());
    Console.WriteLine();
    await CompareDataAsync();
}

async Task RunReplicationExperimentAsync()
{
    Console.WriteLine("\n=== Три последовательных изменения и передача их в Replica ===");
    Console.WriteLine("После каждого изменения сервер увеличивает версию (lsn) и переносит");
    Console.WriteLine("затронутые таблицы во вторую копию. Автомобили: " + string.Join(", ", experimentCars));
    Console.WriteLine("Журнал Replication — в консоли сервера и в пункте 13.\n");

    if (!await ResetAsync())
    {
        return;
    }

    for (int i = 0; i < experimentCars.Length; i++)
    {
        string clientName = $"Client {i + 1}";

        await SendRequestAsync(new Request
        {
            Operation = "CreateRental",
            Data = JsonSerializer.SerializeToElement(new
            {
                fullName = $"Клиент {i + 1}",
                phone = $"37529100000{i + 1}",
                registrationNumber = experimentCars[i],
                startDate = DateTime.UtcNow.Date,
                days = 1,
                simulateError = false,
                clientName
            })
        }, clientName);
    }

    Console.WriteLine("\nСостояние экземпляров:");
    await ShowInstancesAsync();
    Console.WriteLine();
    await CompareDataAsync();
}

async Task ShowInstancesAsync()
{
    Response? response = await SendRequestAsync(EmptyRequest("ShowInstances"), printData: false);
    PrintInstances(response);
}

async Task CompareDataAsync()
{
    Response? response = await SendRequestAsync(EmptyRequest("CompareData"), printData: false);
    PrintDiff(response);
}

async Task SyncDataAsync()
{
    Console.WriteLine("\n=== Полная синхронизация копий ===");
    Console.WriteLine("Источник выбирается по версии данных: обновлённая копия переносит строки в отставшую.\n");

    Response? response = await SendRequestAsync(EmptyRequest("SyncData"), printData: false);

    if (response is { Success: true })
    {
        Console.WriteLine("Проверка после синхронизации:");
        PrintInstances(await SendRequestAsync(EmptyRequest("ShowInstances"), printData: false, printMessage: false));
        PrintDiff(await SendRequestAsync(EmptyRequest("CompareData"), printData: false, printMessage: false));
    }
}

async Task ReplicationLogAsync()
{
    Response? response = await SendRequestAsync(EmptyRequest("ReplicationLog"), printData: false);

    if (response?.Data == null)
    {
        return;
    }

    Console.WriteLine("Журнал сервера (последние события, сверху новые):");

    foreach (JsonElement entry in response.Data.Value.EnumerateArray())
    {
        Console.WriteLine($"  {entry.GetString()}");
    }
}

async Task BrokerStatusAsync()
{
    Response? response = await SendRequestAsync(EmptyRequest("BrokerStatus"), printData: false);

    if (response?.Data == null)
    {
        return;
    }

    Console.WriteLine("Broker хранит подписки; ни клиент, ни Publisher не знают, сколько подписчиков:");

    foreach (JsonElement entry in response.Data.Value.EnumerateArray())
    {
        Console.WriteLine($"  {entry.GetString()}");
    }
}

async Task ClusterCheckAsync()
{
    Console.WriteLine("\n=== Раунд heartbeat: узел опрашивает соседей и при необходимости выбирает лидера ===");
    Console.WriteLine("Узел ждёт ответа соседа 1 с, поэтому отказ видно сразу, а не через интервал опроса.\n");
    await PrintLinesAsync(EmptyRequest("ClusterCheck"), "Состояние узлов после раунда");
}

async Task ClusterProposeAsync()
{
    Console.Write("Значение предложения (Enter — RentalStatus = Active): ");
    string value = Console.ReadLine()?.Trim() ?? "";

    if (value.Length == 0)
    {
        value = "RentalStatus = Active";
    }

    Console.Write("Version, 0 — взять свежую (старое число даст NO от всех узлов): ");

    if (!long.TryParse(Console.ReadLine()?.Trim(), out long version) || version < 0)
    {
        version = 0;
    }

    Response? response = await SendRequestAsync(new Request
    {
        Operation = "ClusterPropose",
        Data = JsonSerializer.SerializeToElement(new { value, version })
    }, printData: false);

    if (response?.Data != null)
    {
        Console.WriteLine("Голоса узлов:");

        foreach (JsonElement entry in response.Data.Value.EnumerateArray())
        {
            Console.WriteLine($"  {entry.GetString()}");
        }
    }
}

async Task ClusterNodeAsync(string operation)
{
    Console.Write("Номер узла (1..5): ");

    if (!int.TryParse(Console.ReadLine()?.Trim(), out int nodeId) || nodeId <= 0)
    {
        Console.WriteLine("Номер узла должен быть целым числом");
        return;
    }

    await PrintLinesAsync(new Request
    {
        Operation = operation,
        Data = JsonSerializer.SerializeToElement(new { nodeId })
    }, "Состояние кластера");
}

async Task PrintLinesAsync(Request request, string title)
{
    Response? response = await SendRequestAsync(request, printData: false);

    if (response?.Data == null)
    {
        return;
    }

    Console.WriteLine($"{title}:");

    foreach (JsonElement entry in response.Data.Value.EnumerateArray())
    {
        Console.WriteLine($"  {entry.GetString()}");
    }
}

async Task<bool> ResetAsync()
{
    Response? response = await SendRequestAsync(EmptyRequest("ResetData"));

    return response is { Success: true };
}

async Task<Response?> ShowStateAsync()
{
    return await SendRequestAsync(EmptyRequest("ShowState"), printMessage: false);
}

static Request EmptyRequest(string operation)
{
    return new Request
    {
        Operation = operation,
        Data = JsonSerializer.SerializeToElement(new { })
    };
}

static void PrintState(Response? response)
{
    if (response?.Data == null)
    {
        Console.WriteLine("Не удалось прочитать состояние базы.");
        return;
    }

    JsonElement data = response.Data.Value;

    if (data.TryGetProperty("lsn", out JsonElement lsn))
    {
        Console.WriteLine($"Экземпляр: {response.Instance}, версия данных (lsn) = {lsn.GetInt64()}");
    }

    Console.WriteLine("Автомобили:");

    if (data.TryGetProperty("cars", out JsonElement cars))
    {
        foreach (JsonElement car in cars.EnumerateArray())
        {
            string registration = car.GetProperty("registrationNumber").GetString() ?? "";
            int available = car.GetProperty("isAvailable").GetBoolean() ? 1 : 0;
            Console.WriteLine($"  {registration}: Available = {available}");
        }
    }

    Console.WriteLine("Аренды:");

    if (!data.TryGetProperty("rentals", out JsonElement rentals) || rentals.GetArrayLength() == 0)
    {
        Console.WriteLine("  (нет)");
        return;
    }

    foreach (JsonElement rental in rentals.EnumerateArray())
    {
        int id = rental.GetProperty("id").GetInt32();
        string registration = rental.GetProperty("registrationNumber").GetString() ?? "";
        string status = rental.GetProperty("status").GetString() ?? "";
        Console.WriteLine($"  #{id} {registration} {status}");
    }
}

static void PrintInstances(Response? response)
{
    if (response?.Data == null)
    {
        Console.WriteLine("Не удалось прочитать состояние экземпляров.");
        return;
    }

    JsonElement data = response.Data.Value;
    string? active = data.TryGetProperty("active", out JsonElement activeElement)
        ? activeElement.GetString()
        : null;

    Console.WriteLine($"Активный экземпляр (через него шёл последний запрос): {active ?? "ни один (оба экземпляра недоступны)"}");

    if (!data.TryGetProperty("instances", out JsonElement instances))
    {
        return;
    }

    foreach (JsonElement instance in instances.EnumerateArray())
    {
        string name = instance.GetProperty("name").GetString() ?? "";
        string state = instance.GetProperty("state").GetString() ?? "";
        long lsn = instance.GetProperty("lsn").GetInt64();
        bool isActive = instance.GetProperty("isActive").GetBoolean();
        string role = isActive ? "активный" : "резерв";

        string content = state == "AVAILABLE"
            ? $"автомобилей {instance.GetProperty("cars").GetArrayLength()}, аренд {instance.GetProperty("rentals").GetInt32()}"
            : "данные недоступны, показана последняя известная версия";

        Console.WriteLine($"  {name,-8} {state,-12} lsn = {lsn,-3} [{role}] {content}");
    }
}

static void PrintDiff(Response? response)
{
    if (response?.Data == null)
    {
        return;
    }

    JsonElement data = response.Data.Value;
    long primaryLsn = data.GetProperty("primaryLsn").GetInt64();
    long replicaLsn = data.GetProperty("replicaLsn").GetInt64();

    Console.WriteLine($"Версии данных: Primary lsn = {primaryLsn}, Replica lsn = {replicaLsn}");

    if (data.GetProperty("isConsistent").GetBoolean())
    {
        Console.WriteLine("Копии согласованы: содержимое и версия совпадают.");
        return;
    }

    Console.WriteLine("Отличия:");

    foreach (JsonElement difference in data.GetProperty("differences").EnumerateArray())
    {
        Console.WriteLine($"  - {difference.GetString()}");
    }
}

async Task<Response?> SendRequestAsync(
    Request request,
    string? label = null,
    bool printMessage = true,
    bool printData = true)
{
    string prefix = label == null ? string.Empty : $"[{label}] ";

    try
    {
        using var client = new TcpClient();
        await client.ConnectAsync(serverAddress, serverPort);

        using NetworkStream stream = client.GetStream();
        using var reader = new StreamReader(stream);
        using var writer = new StreamWriter(stream) { AutoFlush = true };

        string requestJson = JsonSerializer.Serialize(request,
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

        await writer.WriteLineAsync(requestJson);

        string? responseJson = await reader.ReadLineAsync();
        Response? response = JsonSerializer.Deserialize<Response>(responseJson ?? string.Empty,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (response == null)
        {
            Console.WriteLine($"{prefix}Сервер вернул некорректный ответ");
            return null;
        }

        if (printMessage)
        {
            string instance = response.Instance == null ? string.Empty : $" [{response.Instance}]";
            Console.WriteLine($"{prefix}{(response.Success ? "SUCCESS" : "FAIL")}{instance}: {response.Message}");

            if (!string.IsNullOrWhiteSpace(response.Events))
            {
                Console.WriteLine($"{prefix}событие: {response.Events}");
            }

            if (!string.IsNullOrWhiteSpace(response.Consensus))
            {
                Console.WriteLine($"{prefix}консенсус: {response.Consensus}");
            }
        }

        if (printData && response.Success && response.Data.HasValue &&
            request.Operation is "SearchCar" or "CreateRental" or "CreateRentalNoTransaction")
        {
            Console.WriteLine(JsonSerializer.Serialize(
                response.Data.Value,
                new JsonSerializerOptions
                {
                    Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
                    WriteIndented = true
                }));
        }

        return response;
    }
    catch (SocketException)
    {
        Console.WriteLine($"{prefix}Сервер недоступен. Сначала запустите проект Server");
        return null;
    }
    catch (IOException)
    {
        Console.WriteLine($"{prefix}Ошибка сетевого взаимодействия (соединение разорвано)");
        return null;
    }
    catch (JsonException)
    {
        Console.WriteLine($"{prefix}Не удалось обработать ответ сервера");
        return null;
    }
}

static object? GetRequestData(string operation)
{
    if (operation == "SearchCar")
    {
        Console.Write("Локация (Минск или Брест, Enter — любая): ");
        string? location = Console.ReadLine();
        Console.Write("Марка или модель (Enter — любая): ");
        string? brandOrModel = Console.ReadLine();
        return new { location, brandOrModel };
    }

    if (operation == "CreateRental")
    {
        Console.Write("ФИО клиента: ");
        string? fullName = Console.ReadLine();
        Console.Write("Телефон: ");
        string? phone = Console.ReadLine();
        Console.Write("Госномер автомобиля (например, 1234 AB-7): ");
        string? registrationNumber = Console.ReadLine();
        Console.Write("Дата начала (ГГГГ-ММ-ДД): ");

        if (!DateTime.TryParse(Console.ReadLine(), out DateTime startDate))
        {
            Console.WriteLine("Дата указана некорректно");
            return null;
        }

        Console.Write("Количество дней: ");

        if (!int.TryParse(Console.ReadLine(), out int days))
        {
            Console.WriteLine("Количество дней должно быть целым числом");
            return null;
        }

        return new { fullName, phone, registrationNumber, startDate, days, simulateError = false };
    }

    if (operation == "ShowState")
    {
        return new { };
    }

    Console.Write("Номер аренды: ");

    if (!int.TryParse(Console.ReadLine(), out int rentalId))
    {
        Console.WriteLine("Номер аренды должен быть целым числом");
        return null;
    }

    return new { rentalId };
}

static int ReadIntEnvironment(string name, int defaultValue)
{
    string? raw = Environment.GetEnvironmentVariable(name);

    return int.TryParse(raw, out int value) && value > 0 ? value : defaultValue;
}
