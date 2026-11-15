namespace Shared.Cluster;

/// <summary>
/// Операции протокола узлов. Envelope и сериализация общие с Pub/Sub из ЛР №6
/// (PubSubMessage, PubSubJson, LineConnection) — один запрос = одна строка JSON.
/// </summary>
public static class ClusterOperations
{
    /// Проверка доступности: узел отвечает своим состоянием, FAILED-узел молчит.
    public const string Heartbeat = "heartbeat";

    /// Состояние узла — используется соседями на выборах и сервером для таблицы кластера.
    public const string State = "state";

    /// Кандидат объявляет себя: узел отвечает, согласен ли он, а после подтверждения большинством признаёт лидера.
    public const string Elect = "elect";

    /// Лидер предлагает значение, узел отвечает голосом.
    public const string Propose = "propose";

    /// Лидер просит соседа проголосовать: propose — это команда лидеру, vote — запрос голосующему.
    public const string Vote = "vote";

    /// Фиксация согласованного значения: узел принимает версию как зафиксированную.
    public const string Commit = "commit";

    /// Снимок кластера глазами этого узла.
    public const string Info = "info";

    /// Принудительный раунд heartbeat + выборы, если лидера нет. Нужен для демонстрации по нажатию.
    public const string Check = "check";

    // Управление стендом (отказ/восстановление). Работает и на FAILED-узле, иначе остановленный
    // узел нельзя было бы поднять: его состояние меняется командой, а не молчанием.
    public const string Fail = "fail";
    public const string Restore = "restore";

    /// Ответ на любую операцию: в Data — payload конкретного сообщения.
    public const string Result = "result";
}

/// <summary>Взгляд узла на себя или на соседа: id, состояние, роль, зафиксированная версия.</summary>
public class NodeViewMessage
{
    public int Id { get; set; }
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; }

    /// AVAILABLE, FAILED, NO RESPONSE
    public string Status { get; set; } = ClusterStates.Available;

    /// LEADER или FOLLOWER
    public string Role { get; set; } = ClusterStates.Follower;

    /// Какую версию значения узел зафиксировал (0 — ничего не принимал).
    public long CommittedVersion { get; set; }

    public string Endpoint => $"{Host}:{Port}";
}

/// <summary>Объявление кандидата на выборах.</summary>
public class ElectMessage
{
    public int CandidateId { get; set; }

    /// От кого пришло объявление — для журнала.
    public int FromId { get; set; }

    /// false — кандидат только спрашивает согласие, лидер ещё не подтверждён большинством.
    /// true — выборы подтверждены, узел признаёт лидера.
    public bool Confirmed { get; set; }
}

public class ElectAckMessage
{
    /// true — узел признал кандидата (он не слабее уже признанного).
    public bool Accept { get; set; }

    public int LeaderId { get; set; }
    public string Message { get; set; } = string.Empty;
}

/// <summary>Предложение значения. Version — версия данных (lsn), Value — значение предметной области.</summary>
public class ProposeMessage
{
    public long Version { get; set; }
    public string Value { get; set; } = string.Empty;
}

/// <summary>Голос узла. YES только если версия предложения свежее зафиксированной.</summary>
public class VoteMessage
{
    public bool Accept { get; set; }

    /// Как проголосовал узел: YES или NO.
    public string Vote { get; set; } = string.Empty;

    public string Message { get; set; } = string.Empty;
}

/// <summary>Результат голосования, который считает лидер.</summary>
public class ConsensusResultMessage
{
    public bool Achieved { get; set; }

    /// CONSENSUS ACHIEVED / CONSENSUS NOT ACHIEVED
    public string Verdict { get; set; } = string.Empty;

    public int Yes { get; set; }
    public int No { get; set; }
    public int Unanswered { get; set; }
    public int TotalNodes { get; set; }
    public int Majority { get; set; }
    public long Version { get; set; }
    public string Value { get; set; } = string.Empty;
    public int LeaderId { get; set; }

    /// Строки вида «Node 1 -> YES» — их же видно в журнале узлов.
    public List<string> Votes { get; set; } = new();

    /// Причина, если голосование не состоялось вовсе (лидера нет).
    public string Message { get; set; } = string.Empty;
}

/// <summary>Снимок кластера глазами одного узла.</summary>
public class ClusterInfoMessage
{
    public int NodeId { get; set; }
    public string Status { get; set; } = string.Empty;
    public string Role { get; set; } = string.Empty;
    public int LeaderId { get; set; }
    public int TotalNodes { get; set; }
    public int Majority { get; set; }
    public long CommittedVersion { get; set; }
    public List<NodeViewMessage> Nodes { get; set; } = new();
    public List<string> Journal { get; set; } = new();
    public string LastProposal { get; set; } = string.Empty;
}

/// <summary>Команда управления стендом и ответ на неё.</summary>
public class NodeControlMessage
{
    public int NodeId { get; set; }

    /// fail или restore
    public string Action { get; set; } = string.Empty;

    public bool Success { get; set; }

    /// AVAILABLE или FAILED после команды.
    public string Status { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
}
