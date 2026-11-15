namespace Shared.Events;

/// <summary>
/// Событие предметной области. Описывает уже произошедший факт, а не команду что-то сделать:
/// Publisher не знает, кто и знает ли вообще кто-то про это событие.
/// </summary>
public class DomainEvent
{
    public string EventId { get; set; } = Guid.NewGuid().ToString("N")[..8];

    public string EventType { get; set; } = string.Empty;

    public int EntityId { get; set; }

    public int ClientId { get; set; }

    public DateTime Timestamp { get; set; } = DateTime.Now;

    public Dictionary<string, object?> Data { get; set; } = new();

    public override string ToString() => $"{EventType} #{EntityId} (event {EventId}, client {ClientId})";
}
