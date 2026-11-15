using System.Runtime.InteropServices;
using System.Text;
using Nodes;
using Shared.Cluster;
using Shared.Events;

Console.OutputEncoding = Encoding.UTF8;

ClusterNode node;
List<Member> members;
EventJournal journal = new();

try
{
    node = CreateNode(out members, journal);
}
catch (InvalidOperationException ex)
{
    Console.WriteLine(ex.Message);
    return 1;
}

int heartbeatIntervalMs = ReadIntEnvironment("HEARTBEAT_INTERVAL_MS", 1500);

Console.WriteLine($"Узел распределённой системы — ЛР №8. Этот процесс — {node.Name}, всего узлов {members.Count}");
Console.WriteLine($"Состав: {string.Join(", ", members.Select(member => $"{member.Name} = {member.Endpoint}"))}");
Console.WriteLine($"Слушаю порт {node.OwnView().Port}. Heartbeat каждые {heartbeatIntervalMs} мс.");
Console.WriteLine("Правило выборов: лидер — доступный узел с наибольшим ID; выбор подтверждается большинством кластера.");
Console.WriteLine("Отказ узла: узел переходит в FAILED и молчит на heartbeat — соседи фиксируют NO RESPONSE.");

node.Start();
node.StartHeartbeat(heartbeatIntervalMs);

await node.HeartbeatRoundAsync("запуск узла");

if (!Console.IsInputRedirected)
{
    await RunMenu();
}
else
{
    // Контейнер и запуск с перенаправленным вводом: меню нет, узел работает до SIGTERM или Ctrl+C.
    var stopped = new TaskCompletionSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        stopped.TrySetResult();
    };

    // docker stop шлёт SIGTERM, а не Ctrl+C — без этой регистрации узел умирал бы
    // в контейнере без node.Dispose() и без строки «остановлен» в журнале.
    using PosixSignalRegistration sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, context =>
    {
        context.Cancel = true;
        journal.Write(node.Name, "STOPPING, получен SIGTERM (docker stop)");
        stopped.TrySetResult();
    });

    await stopped.Task;
}

node.Dispose();
Console.WriteLine($"{node.Name} остановлен.");
return 0;

async Task RunMenu()
{
    while (true)
    {
        Console.WriteLine($"\n{node.Name} (ЛР №8) — {node.Status}, {node.Role}, лидер Node {node.LeaderId}");
        Console.WriteLine("1  - Состояние узла и взгляд на кластер");
        Console.WriteLine("2  - Раунд heartbeat: проверить соседей, при необходимости выбрать лидера");
        Console.WriteLine("3  - Предложить значение и собрать голоса (проводит только лидер)");
        Console.WriteLine("4  - Отметить этот узел FAILED (смоделировать отказ)");
        Console.WriteLine("5  - Восстановить этот узел (AVAILABLE)");
        Console.WriteLine("6  - Журнал узла");
        Console.WriteLine("0  - Выход");
        Console.Write("Выберите операцию: ");

        string? choice = Console.ReadLine();

        // null — это EOF или Ctrl+Z: без выхода меню печатало бы список бесконечно.
        if (choice == null || choice == "0")
        {
            return;
        }

        switch (choice.Trim())
        {
            case "1":
                PrintStatus();
                break;

            case "2":
                Console.WriteLine($"Раунд heartbeat узла {node.Id}:");
                PrintViews(await node.HeartbeatRoundAsync("меню узла"));
                break;

            case "3":
                await ProposeFromMenu();
                break;

            case "4":
                node.MarkFailed("отказ смоделирован из меню узла");
                break;

            case "5":
                await node.RestoreAsync();
                break;

            case "6":
                foreach (string line in node.Info().Journal)
                {
                    Console.WriteLine(line);
                }

                break;

            default:
                Console.WriteLine("Такого пункта нет — выберите число из списка.");
                break;
        }
    }
}

async Task ProposeFromMenu()
{
    Console.Write("Version (например, 12): ");

    if (!long.TryParse(Console.ReadLine(), out long version) || version <= 0)
    {
        Console.WriteLine("Version должно быть целым числом больше нуля.");
        return;
    }

    Console.Write("Значение (например, RentalStatus = Active): ");
    string value = (Console.ReadLine() ?? string.Empty).Trim();

    if (value.Length == 0)
    {
        Console.WriteLine("Значение не введено — предложение не отправлено.");
        return;
    }

    ConsensusResultMessage result = await node.VoteAsync(version, value);

    Console.WriteLine(result.Message.Length > 0 ? result.Message : result.Verdict);
    Console.WriteLine($"YES = {result.Yes}, NO = {result.No}, без ответа = {result.Unanswered}, Majority = {result.Majority}");
}

void PrintStatus()
{
    ClusterInfoMessage info = node.Info();

    Console.WriteLine($"{node.Name}: {info.Status}, {info.Role}, лидер Node {info.LeaderId}, " +
                      $"зафиксировано Version={info.CommittedVersion}, большинство {info.Majority} из {info.TotalNodes}");

    if (info.LastProposal.Length > 0)
    {
        Console.WriteLine($"последнее предложение: {info.LastProposal}");
    }

    PrintViews(info.Nodes);
}

void PrintViews(List<NodeViewMessage> views)
{
    foreach (NodeViewMessage view in views)
    {
        string role = view.Status != ClusterStates.Available
            ? "—"
            : view.Id == node.LeaderId
                ? ClusterStates.Leader
                : ClusterStates.Follower;

        Console.WriteLine($"  {($"Node {view.Id} {view.Status}"),-28} {role,-9} {view.Endpoint,-18} Version = {view.CommittedVersion}");
    }
}

static ClusterNode CreateNode(out List<Member> members, EventJournal journal)
{
    members = Member.Parse(Environment.GetEnvironmentVariable("CLUSTER_MEMBERS"));

    string? rawId = Environment.GetEnvironmentVariable("NODE_ID");

    if (!int.TryParse(rawId, out int id))
    {
        throw new InvalidOperationException(
            $"NODE_ID должен быть целым числом 1..{members.Count}, получено '{rawId}'. Пример: NODE_ID=3 Node.exe (в PowerShell: $env:NODE_ID='3').");
    }

    Member? self = members.FirstOrDefault(member => member.Id == id);

    if (self == null)
    {
        throw new InvalidOperationException(
            $"NODE_ID={id} нет в составе кластера: {string.Join(", ", members.Select(member => $"{member.Name} = {member.Endpoint}"))}");
    }

    return new ClusterNode(self, members, journal);
}

static int ReadIntEnvironment(string name, int defaultValue)
{
    string? raw = Environment.GetEnvironmentVariable(name);

    return int.TryParse(raw, out int value) && value > 0 ? value : defaultValue;
}
