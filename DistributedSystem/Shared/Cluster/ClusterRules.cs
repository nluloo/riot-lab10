namespace Shared.Cluster;

/// <summary>Состояние узла и его роль — строки такие же, как в журнале, чтобы лог читался без перевода.</summary>
public static class ClusterStates
{
    public const string Available = "AVAILABLE";
    public const string Failed = "FAILED";

    /// Узел не ответил: процесс остановлен или молчит дольше таймаута.
    public const string NoResponse = "NO RESPONSE";

    public const string Leader = "LEADER";
    public const string Follower = "FOLLOWER";
}

/// <summary>
/// Чистые правила консенсуса без сети: большинство, выбор лидера, подсчёт голосов.
/// Вынесены из узлов, потому что именно на них проверяются тесты Требование 4.
/// </summary>
public static class ClusterRules
{
    /// Большинство от полного состава кластера: 5 -> 3, 4 -> 3, 3 -> 2.
    /// Считается от всех узлов, а не от доступных, иначе при отказе половины
    /// большинство стало бы доступнее и решение приняли бы двое из пяти.
    public static int Majority(int totalNodes) => totalNodes / 2 + 1;

    /// Лидером становится доступный узел с наибольшим идентификатором (правило из методички).
    /// 0 — подходящего узла нет.
    public static int PickLeader(IEnumerable<NodeViewMessage> views) => views
        .Where(view => view.Status == ClusterStates.Available)
        .Select(view => view.Id)
        .DefaultIfEmpty(0)
        .Max();

    /// Выбор подтверждены, только если кандидата признало большинство всего кластера.
    public static bool IsElectionConfirmed(int acknowledgements, int totalNodes) =>
        acknowledgements >= Majority(totalNodes);

    /// Подсчёт голосов лидера. Отсутствующий ответ — это не NO: он просто не увеличивает YES.
    public static ConsensusResultMessage Tally(
        int yes,
        int no,
        int unanswered,
        int totalNodes,
        long version,
        string value,
        int leaderId)
    {
        int majority = Majority(totalNodes);

        return new ConsensusResultMessage
        {
            Achieved = yes >= majority,
            Verdict = yes >= majority ? "CONSENSUS ACHIEVED" : "CONSENSUS NOT ACHIEVED",
            Yes = yes,
            No = no,
            Unanswered = unanswered,
            TotalNodes = totalNodes,
            Majority = majority,
            Version = version,
            Value = value,
            LeaderId = leaderId
        };
    }
}
