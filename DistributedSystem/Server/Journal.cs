using System.Text;

namespace Server;

/// <summary>
/// Журнал событий сервера: строки пишутся в консоль и сохраняются в буфере
/// </summary>
public class Journal
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
