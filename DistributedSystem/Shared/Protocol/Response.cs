using System.Text.Json;

namespace Shared.Protocol;

public class Response
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;

    /// Экземпляр данных, через который сервер выполнил запрос: Primary или Replica.
    /// null — когда экземпляр не был выбран (оба недоступны) или операция его не использует.
    public string? Instance { get; set; }

    /// Что произошло с событием операции: в какие Broker ушло, сколько подписчиков его получило.
    /// null — когда операция события не формирует.
    public string? Events { get; set; }

    /// Итог согласования с кластером узлов: лидер, голоса, большинство, вердикт.
    /// null — когда операция консенсуса не требовала или узлы недоступны (сервер деградирует).
    public string? Consensus { get; set; }

    public JsonElement? Data { get; set; }
}
