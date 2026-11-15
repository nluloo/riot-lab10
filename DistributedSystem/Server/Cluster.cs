using Shared.Cluster;
using Shared.Events;

namespace Server;

/// <summary>
/// Кластер узлов глазами сервера. Сервер не выбирает лидера и не считает голоса сам:
/// он спрашивает узлы и передаёт предложение тому, кого узлы признали лидером.
/// Узлы серверу не нужны для работы — если они не отвечают, сервер выполняет операцию как в ЛР №6.
/// </summary>
public class Cluster
{
    private const int ConnectTimeoutMs = 400;
    private const int StateTimeoutMs = 1200;

    /// check и restore запускают в узле полный раунд heartbeat с выборами, поэтому ждут дольше.
    private const int RoundTimeoutMs = 9000;

    /// Лидер сам опрашивает четверых соседей и ждёт их голоса.
    private const int ProposeTimeoutMs = 12000;

    private readonly List<Member> _members;
    private readonly Journal _journal;

    public Cluster(string? membersSetting, Journal journal)
    {
        _journal = journal;
        _members = string.IsNullOrWhiteSpace(membersSetting) ? new List<Member>() : Member.Parse(membersSetting);
    }

    public bool Enabled => _members.Count > 0;

    public int TotalNodes => _members.Count;

    public int Majority => ClusterRules.Majority(TotalNodes);

    public IReadOnlyList<Member> Members => _members;

    /// Взгляд кластера на себя: состояние и роль каждого узла. Молчание узла — NO RESPONSE.
    public async Task<List<NodeViewMessage>> ViewsAsync()
    {
        List<(Member Member, PubSubMessage? Reply)> replies = await ClusterTransport.RequestManyAsync(
            _members, ClusterOperations.State, new { }, ConnectTimeoutMs, StateTimeoutMs);

        return ClusterTransport.Views(_members, replies);
    }

    /// Лидер — узел, который сам объявил себя лидером. Если таких несколько, это раскол: видно в журнале.
    public int LeaderOf(List<NodeViewMessage> views)
    {
        List<int> leaders = views
            .Where(view => view.Status == ClusterStates.Available && view.Role == ClusterStates.Leader)
            .Select(view => view.Id)
            .OrderByDescending(id => id)
            .ToList();

        if (leaders.Count > 1)
        {
            _journal.Write("Cluster",
                $"РАСКОЛ: лидерами объявили себя {string.Join(", ", leaders.Select(id => $"Node {id}"))} — узлы видят разный состав кластера");
        }

        return leaders.Count == 0 ? 0 : leaders[0];
    }

    /// Версия следующего предложения: заведомо свежее любой зафиксированной в кластере.
    public static long NextVersionOf(List<NodeViewMessage> views) =>
        views.Count == 0 ? 1 : views.Max(view => view.CommittedVersion) + 1;

    /// Раунд heartbeat сразу на каждом узле. Один узел отказ заметит, но его кандидата остальные
    /// не признали бы, пока сами не обновили взгляд на кластер, — перевыборы растянулись бы на
    /// несколько интервалов опроса. Общий раунд даёт согласие за один проход.
    public async Task<ClusterInfoMessage?> CheckAsync(List<NodeViewMessage>? views = null)
    {
        views ??= await ViewsAsync();

        List<Member> targets = _members
            .Where(member => views.Any(view => view.Id == member.Id && view.Status == ClusterStates.Available))
            .ToList();

        if (targets.Count == 0)
        {
            targets = _members;
        }

        List<(Member Member, PubSubMessage? Reply)> replies = await ClusterTransport.RequestManyAsync(
            targets, ClusterOperations.Check, new { }, ConnectTimeoutMs, RoundTimeoutMs);

        ClusterInfoMessage? info = replies
            .Select(reply => reply.Reply == null ? null : PubSubJson.Payload<ClusterInfoMessage>(reply.Reply))
            .Where(message => message != null)
            .OrderByDescending(message => message!.LeaderId)
            .FirstOrDefault();

        int available = info == null
            ? 0
            : info.Nodes.Count(node => node.Status == ClusterStates.Available);

        _journal.Write("Cluster", info == null
            ? $"CHECK FAIL: ни один узел из {targets.Count} не ответил за {RoundTimeoutMs} мс"
            : $"CHECK на {targets.Count} узлах: лидер {(info.LeaderId == 0 ? "не выбран" : $"Node {info.LeaderId}")}, " +
              $"доступно {available}/{info.TotalNodes}, большинство {info.Majority}");

        return info;
    }

    /// Предложение значения кластеру.
    /// null — узлы не отвечают вовсе: сервер деградирует и выполняет операцию без консенсуса.
    /// Ответ с Achieved == false — узлы живые, но большинства нет: решение принимать нельзя.
    public async Task<ConsensusResultMessage?> ProposeAsync(string value, long? fixedVersion = null)
    {
        if (!Enabled)
        {
            return null;
        }

        List<NodeViewMessage> views = await ViewsAsync();
        int available = views.Count(view => view.Status == ClusterStates.Available);

        if (available == 0)
        {
            _journal.Write("Cluster", "CONSENSUS SKIPPED: узлы кластера не отвечают — операция выполняется без консенсуса (как в ЛР №6)");
            return null;
        }

        long version = fixedVersion ?? NextVersionOf(views);

        int leader = LeaderOf(views);

        if (leader == 0)
        {
            await CheckAsync(views);
            views = await ViewsAsync();
            leader = LeaderOf(views);
        }

        if (leader == 0)
        {
            ConsensusResultMessage withoutLeader = ClusterRules.Tally(
                0, 0, TotalNodes - available, TotalNodes, version, value, 0);

            withoutLeader.Message =
                $"Лидера нет: выборам не хватило большинства (доступно {available} из {TotalNodes}, нужно {Majority})";

            _journal.Write("Cluster", $"PROPOSE Version={version} -> {withoutLeader.Verdict}: {withoutLeader.Message}");

            return withoutLeader;
        }

        Member leaderMember = _members.First(member => member.Id == leader);

        PubSubMessage? reply = await ClusterTransport.RequestAsync(
            leaderMember, ClusterOperations.Propose, new ProposeMessage { Version = version, Value = value },
            ConnectTimeoutMs, ProposeTimeoutMs);

        ConsensusResultMessage? result = reply == null ? null : PubSubJson.Payload<ConsensusResultMessage>(reply);

        if (result == null)
        {
            // Лидер мог выйти из строя между двумя опросами: один раз повторяем через проверку доступности.
            _journal.Write("Cluster", $"PROPOSE FAIL: лидер Node {leader} не ответил за {ProposeTimeoutMs} мс, перевыборы");
            await CheckAsync();

            List<NodeViewMessage> afterRetry = await ViewsAsync();
            int retryLeader = LeaderOf(afterRetry);

            if (retryLeader == 0)
            {
                return ClusterRules.Tally(0, 0, TotalNodes - afterRetry.Count(view => view.Status == ClusterStates.Available),
                    TotalNodes, version, value, 0);
            }

            PubSubMessage? retryReply = await ClusterTransport.RequestAsync(
                _members.First(member => member.Id == retryLeader),
                ClusterOperations.Propose,
                new ProposeMessage { Version = version, Value = value },
                ConnectTimeoutMs,
                ProposeTimeoutMs);

            result = retryReply == null ? null : PubSubJson.Payload<ConsensusResultMessage>(retryReply);

            if (result == null)
            {
                return ClusterRules.Tally(0, 0, TotalNodes - afterRetry.Count(view => view.Status == ClusterStates.Available),
                    TotalNodes, version, value, retryLeader);
            }
        }

        _journal.Write("Cluster",
            $"Leader Node {result.LeaderId} -> PROPOSE Version={version}: {result.Verdict}, " +
            $"YES = {result.Yes}, NO = {result.No}, без ответа = {result.Unanswered}, Majority = {result.Majority}, " +
            $"доступно {available}/{TotalNodes}");

        return result;
    }

    /// Отказ или восстановление узла. Команда доходит и до FAILED-узла — иначе поднять его было бы нельзя.
    public async Task<NodeControlMessage?> ControlAsync(int nodeId, bool fail)
    {
        Member? member = _members.FirstOrDefault(other => other.Id == nodeId);

        if (member == null)
        {
            _journal.Write("Cluster", $"CONTROL FAIL: узла Node {nodeId} нет в составе ({string.Join(", ", _members.Select(other => other.Id))})");
            return null;
        }

        string operation = fail ? ClusterOperations.Fail : ClusterOperations.Restore;

        PubSubMessage? reply = await ClusterTransport.RequestAsync(
            member, operation, new NodeControlMessage { NodeId = nodeId, Action = operation },
            ConnectTimeoutMs, RoundTimeoutMs);

        NodeControlMessage? result = reply == null ? null : PubSubJson.Payload<NodeControlMessage>(reply);

        _journal.Write("Cluster", result == null
            ? $"CONTROL FAIL: Node {nodeId} не ответил на '{operation}'"
            : $"CONTROL Node {nodeId}: {result.Message} (состояние {result.Status})");

        return result;
    }

    /// Эксперименты 1-4 из Части 6: отказ 0, 1, 2 и 3 узлов и попытка принять решение в каждом.
    public async Task<List<string>> ExperimentsAsync()
    {
        var lines = new List<string>();
        int[][] failures =
        {
            Array.Empty<int>(),
            new[] { 5 },
            new[] { 4, 5 },
            new[] { 3, 4, 5 }
        };

        for (int index = 0; index < failures.Length; index++)
        {
            await RestoreAllAsync();

            foreach (int nodeId in failures[index].Where(id => _members.Any(member => member.Id == id)))
            {
                await ControlAsync(nodeId, fail: true);
            }

            await CheckAsync();

            int available = (await ViewsAsync()).Count(view => view.Status == ClusterStates.Available);

            ConsensusResultMessage? result = await ProposeAsync($"RentalStatus = Active (эксперимент {index + 1})");

            string line = $"Эксперимент {index + 1}: отказ {failures[index].Length} узл., доступно {available}/{TotalNodes}, " +
                          $"большинство {Majority}, " +
                          $"лидер {(result == null || result.LeaderId == 0 ? "не выбран" : $"Node {result.LeaderId}")}, " +
                          $"YES = {result?.Yes ?? 0} -> {result?.Verdict ?? "кластер не отвечает"}";

            lines.Add(line);
            _journal.Write("Cluster", line);
        }

        await RestoreAllAsync();
        await CheckAsync();

        return lines;
    }

    /// Строки состояния для клиента: таблица узлов, лидер, большинство, последнее предложение.
    public async Task<List<string>> StatusLinesAsync()
    {
        List<NodeViewMessage> views = await ViewsAsync();
        int leader = LeaderOf(views);
        int available = views.Count(view => view.Status == ClusterStates.Available);

        var lines = new List<string>();

        foreach (NodeViewMessage view in views)
        {
            string role = view.Status != ClusterStates.Available
                ? "—"
                : view.Id == leader ? ClusterStates.Leader : ClusterStates.Follower;

            lines.Add($"  {view.Id,-3} {view.Status,-12} {role,-9} {view.Endpoint,-18} Version = {view.CommittedVersion}");
        }

        lines.Add($" Leader: {(leader == 0 ? "не выбран" : $"Node {leader}")}. " +
                  $"Доступно {available}/{TotalNodes}, большинство {Majority} " +
                  $"({(available >= Majority ? "решение возможно" : "решение невозможно")}).");

        ClusterInfoMessage? info = await AskInfoAsync(leader != 0 ? leader : views.FirstOrDefault()?.Id ?? 0);

        if (info != null)
        {
            lines.Add($" Зафиксировано Version={info.CommittedVersion}. Последнее предложение: " +
                      $"{(string.IsNullOrEmpty(info.LastProposal) ? "не было" : info.LastProposal)}");
        }

        _journal.Write("Cluster", $"STATUS: лидер Node {leader}, доступно {available}/{TotalNodes}, большинство {Majority}");

        return lines;
    }

    private async Task<ClusterInfoMessage?> AskInfoAsync(int nodeId)
    {
        Member? member = _members.FirstOrDefault(other => other.Id == nodeId);

        if (member == null)
        {
            return null;
        }

        PubSubMessage? reply = await ClusterTransport.RequestAsync(
            member, ClusterOperations.Info, new { }, ConnectTimeoutMs, StateTimeoutMs);

        return reply == null ? null : PubSubJson.Payload<ClusterInfoMessage>(reply);
    }

    private async Task RestoreAllAsync()
    {
        await ClusterTransport.RequestManyAsync(
            _members, ClusterOperations.Restore, new NodeControlMessage { Action = ClusterOperations.Restore },
            ConnectTimeoutMs, RoundTimeoutMs);
    }
}
