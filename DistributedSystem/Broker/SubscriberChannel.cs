using Shared.Events;

namespace Broker;

/// <summary>
/// Канал подписчика на стороне Broker. Один класс и для настоящего TCP-соединения, и для тестовой
/// заглушки: у заглушки просто свой делегат Send, а Online/Reconnectable задаются напрямую.
/// </summary>
public class SubscriberChannel
{
    public SubscriberChannel(string name)
    {
        Name = name;
    }

    public string Name { get; }

    /// Идентификатор подписки — сам тип события.
    public List<string> EventTypes { get; } = new();

    public bool Online { get; set; }

    /// true — Broker копит для него события в очереди и повторяет доставку после восстановления.
    public bool Reconnectable { get; set; }

    /// null — соединения нет. Иначе: отправить событие подписчику и дождаться результата обработки.
    public Func<DeliveryMessage, CancellationToken, Task<DeliveryResultMessage>>? Send { get; set; }

    public DateTime LastSeen { get; set; } = DateTime.Now;
}
