using System.Net.Sockets;
using System.Text;

namespace Shared.Events;

/// <summary>
/// Соединение построчного протокола: TCP, одна строка JSON в одну сторону.
/// Используется всеми тремя процессами: Publisher → Broker и Broker → Subscriber.
/// </summary>
public class LineConnection : IDisposable
{
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly TcpClient _client;
    private readonly StreamReader _reader;
    private readonly StreamWriter _writer;

    private LineConnection(TcpClient client)
    {
        _client = client;
        _client.NoDelay = true;
        _reader = new StreamReader(client.GetStream(), Encoding.UTF8);
        _writer = new StreamWriter(client.GetStream(), Encoding.UTF8) { AutoFlush = true };
    }

    public bool IsOpen => _client.Connected;

    /// Соединение со стороны принимающей: уже установленный TcpClient, представляться не нужно.
    public static LineConnection FromAccepted(TcpClient client) => new(client);

    public static LineConnection Connect(string host, int port, int connectTimeoutMs)
    {
        var client = new TcpClient();

        try
        {
            client.ConnectAsync(host, port)
                .WaitAsync(TimeSpan.FromMilliseconds(connectTimeoutMs))
                .GetAwaiter()
                .GetResult();
        }
        catch (Exception ex) when (ex is SocketException or TimeoutException)
        {
            client.Dispose();
            throw new PubSubTransportException($"Не удалось подключиться к {host}:{port}: {ex.Message}", ex);
        }

        return new LineConnection(client);
    }

    public async Task SendAsync<T>(T message, CancellationToken cancellationToken)
    {
        string json = PubSubJson.Serialize(message!);

        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            await _writer.WriteLineAsync(json.AsMemory(), cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// null — соединение закрыто второй стороной (Broker или Subscriber остановлен).
    public async Task<string?> ReadLineAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _reader.ReadLineAsync(cancellationToken);
        }
        catch (IOException)
        {
            return null;
        }
        catch (SocketException)
        {
            return null;
        }
    }

    // Синхронные обёртки: Publisher живёт в том же синхронном стиле, что работа сервера с PostgreSQL,
    // но с жёстким таймаутом — недоступный Broker не должен удерживать запрос клиента.
    public void SendSync(object message, int timeoutMs)
    {
        using var cts = new CancellationTokenSource(timeoutMs);

        try
        {
            SendAsync(message, cts.Token).GetAwaiter().GetResult();
        }
        catch (OperationCanceledException)
        {
            throw new PubSubTransportException($"не удалось отправить сообщение за {timeoutMs} мс");
        }
    }

    public string? ReadLineSync(int timeoutMs)
    {
        using var cts = new CancellationTokenSource(timeoutMs);

        try
        {
            return ReadLineAsync(cts.Token).GetAwaiter().GetResult();
        }
        catch (OperationCanceledException)
        {
            throw new PubSubTransportException($"Broker не ответил за {timeoutMs} мс");
        }
    }

    public void Close() => Dispose();

    public void Dispose()
    {
        _writeLock.Dispose();
        _reader.Dispose();
        _writer.Dispose();
        _client.Dispose();
    }
}

/// <summary>Ошибка транспорта Pub/Sub: соединение недоступно или разорвано.</summary>
public class PubSubTransportException : Exception
{
    public PubSubTransportException(string message, Exception? inner = null) : base(message, inner)
    {
    }
}
