using Shared.Events;

namespace Broker;

/// <summary>
/// Реестр подписок Broker: кто подключён и какие типы событий каждый получает.
/// Publisher знает только Broker — состав подписчиков здесь, а не в Publisher.
/// </summary>
public class SubscriptionRegistry
{
    private readonly object _sync = new();
    private readonly Dictionary<string, SubscriberChannel> _byName = new(StringComparer.OrdinalIgnoreCase);

    public int Count
    {
        get
        {
            lock (_sync)
            {
                return _byName.Count;
            }
        }
    }

    public SubscriberChannel? Find(string name)
    {
        lock (_sync)
        {
            return _byName.TryGetValue(name, out SubscriberChannel? channel) ? channel : null;
        }
    }

    /// Новое подключение подписчика с тем же именем заменяет старое соединение.
    public SubscriberChannel Add(SubscriberChannel channel)
    {
        lock (_sync)
        {
            _byName[channel.Name] = channel;
            return channel;
        }
    }

    public void Remove(string name)
    {
        lock (_sync)
        {
            _byName.Remove(name);
        }
    }

    public IReadOnlyList<string> Names()
    {
        lock (_sync)
        {
            return _byName.Keys.OrderBy(name => name, StringComparer.OrdinalIgnoreCase).ToList();
        }
    }

    /// Подписка на типы событий. Отдельного вызова Publisher не требует — Broker принимает решение сам.
    public SubscriptionResult Subscribe(string name, IEnumerable<string> eventTypes)
    {
        var result = new SubscriptionResult();
        List<string> requested = eventTypes.ToList();

        if (requested.Count == 0)
        {
            result.Status = "NO_EVENT_TYPES";
            result.Message = "Не указан ни один тип события — подписка не зарегистрирована";
            return result;
        }

        lock (_sync)
        {
            if (!_byName.TryGetValue(name, out SubscriberChannel? channel))
            {
                result.Status = "UNKNOWN_SUBSCRIBER";
                result.Message = $"Подписчик '{name}' не зарегистрирован в Broker";
                return result;
            }

            foreach (string eventType in requested)
            {
                if (!EventTypes.IsKnown(eventType))
                {
                    result.Unknown.Add(eventType);
                    continue;
                }

                if (!channel.EventTypes.Contains(eventType, StringComparer.OrdinalIgnoreCase))
                {
                    channel.EventTypes.Add(eventType);
                }

                result.Applied.Add(eventType);
            }

            result.SubscriberFound = true;
            result.Success = result.Applied.Count > 0;
            result.Status = result.Success
                ? (result.Unknown.Count > 0 ? "PARTIAL" : "OK")
                : "UNKNOWN_EVENT_TYPE";

            result.Message = result.Success
                ? $"{name}: {string.Join(", ", channel.EventTypes)}"
                : $"Неизвестный тип события: {string.Join(", ", result.Unknown)}";
            result.Types = channel.EventTypes.ToList();
        }

        return result;
    }

    public IReadOnlyList<string> UnsubscribeAll(string name)
    {
        lock (_sync)
        {
            if (!_byName.TryGetValue(name, out SubscriberChannel? channel))
            {
                return Array.Empty<string>();
            }

            List<string> removed = channel.EventTypes.ToList();
            channel.EventTypes.Clear();
            return removed;
        }
    }

    public IReadOnlyList<string> EventTypesOf(string name)
    {
        lock (_sync)
        {
            return _byName.TryGetValue(name, out SubscriberChannel? channel)
                ? channel.EventTypes.ToList()
                : Array.Empty<string>();
        }
    }

    public List<SubscriberChannel> GetTargets(string eventType)
    {
        lock (_sync)
        {
            return _byName.Values
                .Where(channel => channel.EventTypes.Contains(eventType, StringComparer.OrdinalIgnoreCase))
                .OrderBy(channel => channel.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
    }

    public List<string> DescribeSubscriptions()
    {
        lock (_sync)
        {
            return _byName.Values
                .OrderBy(channel => channel.Name, StringComparer.OrdinalIgnoreCase)
                .Select(channel => $"{channel.Name}: {(channel.EventTypes.Count > 0 ? string.Join(", ", channel.EventTypes) : "нет подписки")}")
                .ToList();
        }
    }

    public List<string> DescribeStatuses()
    {
        lock (_sync)
        {
            return _byName.Values
                .OrderBy(channel => channel.Name, StringComparer.OrdinalIgnoreCase)
                .Select(channel => $"{channel.Name}: {(channel.Online ? "ONLINE" : "OFFLINE")}{(channel.Reconnectable ? ", очередь" : ", без очереди")}")
                .ToList();
        }
    }
}

/// <summary>Результат регистрации подписки — его Broker отвечает подписчику и пишет в журнал.</summary>
public class SubscriptionResult
{
    public bool Success { get; set; }
    public bool SubscriberFound { get; set; }

    /// OK, PARTIAL, UNKNOWN_SUBSCRIBER, UNKNOWN_EVENT_TYPE, NO_EVENT_TYPES
    public string Status { get; set; } = string.Empty;

    public string Message { get; set; } = string.Empty;
    public List<string> Applied { get; set; } = new();
    public List<string> Unknown { get; set; } = new();
    public List<string> Types { get; set; } = new();
}
