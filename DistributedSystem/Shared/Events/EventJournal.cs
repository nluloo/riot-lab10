using System.Text;

namespace Shared.Events;

/// <summary>
/// Журнал процессов Pub/Sub: строки формата «[HH:mm:ss.fff] Кто -> Событие».
/// Тот же формат, что и журнал сервера в ЛР №5, поэтому логи всех процессов читаются вместе.
/// </summary>
public class EventJournal
{
    private const int MaxEntries = 300;

    private readonly object _sync = new();
    private readonly Queue<string> _entries = new();

    public void Write(string scope, string message)
    {
        string line = $"[{DateTime.Now:HH:mm:ss.fff}] {scope} -> {message}";

        lock (_sync)
        {
            Console.WriteLine(line);
            _entries.Enqueue(line);

            while (_entries.Count > MaxEntries)
            {
                _entries.Dequeue();
            }
        }
    }

    public List<string> Recent(int count)
    {
        lock (_sync)
        {
            return _entries.ToArray()
                .Reverse()
                .Take(count)
                .ToList();
        }
    }
}
