using System.Net.Sockets;
using System.Text.Json;
using Shared.Events;

namespace Shared.Cluster;

/// <summary>
/// Транспорт кластера узлов: одно сообщение = одно новое соединение = одна строка JSON в каждую сторону.
/// Тот же стиль, что у Publisher и Broker в ЛР №6, поэтому LineConnection и PubSubJson переиспользуются как есть.
/// </summary>
public static class ClusterTransport
{
    /// null означает «ответа нет»: не удалось подключиться, соединение оборвано или узел промолчал до таймаута.
    /// Для кластера это штатный результат, а не исключение — так отказ узла не рвёт раунд heartbeat.
    public static async Task<PubSubMessage?> RequestAsync(
        Member target,
        string operation,
        object data,
        int connectTimeoutMs,
        int replyTimeoutMs,
        CancellationToken cancellationToken = default)
    {
        try
        {
            using LineConnection connection = LineConnection.Connect(target.Host, target.Port, connectTimeoutMs);

            await connection.SendAsync(PubSubJson.Message(operation, data), cancellationToken);

            using var timeout = new CancellationTokenSource(replyTimeoutMs);
            string? line = await connection.ReadLineAsync(timeout.Token);

            return line == null ? null : PubSubJson.Deserialize<PubSubMessage>(line);
        }
        catch (Exception ex) when (ex is PubSubTransportException or IOException or SocketException
                or OperationCanceledException or TimeoutException or JsonException or ObjectDisposedException)
        {
            return null;
        }
    }

    /// Опрос всех участников одновременно: раунд heartbeat стоит столько, сколько самый медленный узел,
    /// а не суммы по всем.
    public static async Task<List<(Member Member, PubSubMessage? Reply)>> RequestManyAsync(
        IEnumerable<Member> targets,
        string operation,
        object data,
        int connectTimeoutMs,
        int replyTimeoutMs,
        CancellationToken cancellationToken = default)
    {
        List<Task<(Member Member, PubSubMessage? Reply)>> pending = targets
            .Select(async target => (target, await RequestAsync(target, operation, data, connectTimeoutMs, replyTimeoutMs, cancellationToken)))
            .ToList();

        await Task.WhenAll(pending);

        return pending.Select(task => task.Result).ToList();
    }

    /// Тот же вопрос, заданный всеми участниками: id узла, состояние, роль, зафиксированная версия.
    public static List<NodeViewMessage> Views(
        IEnumerable<Member> targets,
        List<(Member Member, PubSubMessage? Reply)> replies)
    {
        var views = new List<NodeViewMessage>();

        foreach ((Member member, PubSubMessage? reply) in replies)
        {
            NodeViewMessage? answer = reply == null ? null : PubSubJson.Payload<NodeViewMessage>(reply);

            views.Add(new NodeViewMessage
            {
                Id = member.Id,
                Host = member.Host,
                Port = member.Port,
                Status = answer?.Status ?? ClusterStates.NoResponse,
                Role = answer?.Role ?? string.Empty,
                CommittedVersion = answer?.CommittedVersion ?? 0
            });
        }

        return views.OrderBy(view => view.Id).ToList();
    }
}
