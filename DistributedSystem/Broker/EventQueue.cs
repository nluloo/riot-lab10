using Shared.Events;

namespace Broker;

/// <summary>
/// Временное хранение событий в Broker (Часть 5, требование 2): подписчик недоступен —
/// событие не теряется, а ждёт повторной доставки. Очередь ограничена, чтобы Broker не рос бесконечно.
/// </summary>
public class EventQueue
{
    public const int PerSubscriberLimit = 50;

    /// Сколько раз событие повторно пытаются отдать из очереди. Без потолка недоступный
    /// подписчик зациклил бы очередь: насос пытался бы доставлять одно и то же вечно.
    public const int MaxRedeliveries = 5;

    private readonly object _sync = new();
    private readonly Dictionary<string, List<DomainEvent>> _byName = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int> _redeliveries = new(StringComparer.OrdinalIgnoreCase);

    /// false — очередь этого подписчика переполнена, событие придётся отбросить.
    public bool Append(string name, DomainEvent evt)
    {
        lock (_sync)
        {
            if (!_byName.TryGetValue(name, out List<DomainEvent>? pending))
            {
                pending = new List<DomainEvent>();
                _byName[name] = pending;
            }

            if (pending.Count >= PerSubscriberLimit)
            {
                return false;
            }

            pending.Add(evt);
            return true;
        }
    }

    public DomainEvent? Peek(string name)
    {
        lock (_sync)
        {
            return _byName.TryGetValue(name, out List<DomainEvent>? pending) && pending.Count > 0
                ? pending[0]
                : null;
        }
    }

    /// Увеличивает счётчик повторов события. false — лимит MaxRedeliveries исчерпан, событие пора отбросить.
    public bool TryRedeliver(string name, DomainEvent evt)
    {
        lock (_sync)
        {
            string key = QueueKey(name, evt.EventId);
            _redeliveries.TryGetValue(key, out int count);
            _redeliveries[key] = ++count;

            return count <= MaxRedeliveries;
        }
    }

    public void Complete(string name, string eventId)
    {
        lock (_sync)
        {
            _redeliveries.Remove(QueueKey(name, eventId));

            if (_byName.TryGetValue(name, out List<DomainEvent>? pending))
            {
                pending.RemoveAll(evt => evt.EventId == eventId);

                if (pending.Count == 0)
                {
                    _byName.Remove(name);
                }
            }
        }
    }

    public int Count(string name)
    {
        lock (_sync)
        {
            return _byName.TryGetValue(name, out List<DomainEvent>? pending) ? pending.Count : 0;
        }
    }

    public int TotalCount
    {
        get
        {
            lock (_sync)
            {
                return _byName.Values.Sum(pending => pending.Count);
            }
        }
    }

    public IReadOnlyList<string> Names()
    {
        lock (_sync)
        {
            return _byName.Keys.OrderBy(name => name, StringComparer.OrdinalIgnoreCase).ToList();
        }
    }

    public void Clear(string name)
    {
        lock (_sync)
        {
            _byName.Remove(name);

            foreach (string key in _redeliveries.Keys
                         .Where(key => key.StartsWith($"{name}|", StringComparison.OrdinalIgnoreCase))
                         .ToList())
            {
                _redeliveries.Remove(key);
            }
        }
    }

    private static string QueueKey(string name, string eventId) => $"{name}|{eventId}";
}
