namespace Shared.Protocol;

/// <summary>Данные запроса ClusterPropose: значение и необязательная версия (0 — версию берёт кластер).</summary>
public class ClusterProposeData
{
    public string Value { get; set; } = string.Empty;

    /// Передать явно, чтобы показать отрицательное голосование на устаревшем значении.
    public long Version { get; set; }
}

/// <summary>Данные запросов ClusterFailNode и ClusterRestoreNode.</summary>
public class ClusterNodeData
{
    public int NodeId { get; set; }
}
