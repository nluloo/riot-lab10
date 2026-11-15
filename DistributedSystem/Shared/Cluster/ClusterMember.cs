namespace Shared.Cluster;

/// <summary>Участник кластера: id и адрес. Состав задаёт переменная CLUSTER_MEMBERS.</summary>
public class Member
{
    public Member(int id, string host, int port)
    {
        Id = id;
        Host = host;
        Port = port;
    }

    public int Id { get; }

    public string Host { get; }

    public int Port { get; }

    /// Имя совпадает со строками журнала из методички: «Node 5».
    public string Name => $"Node {Id}";

    public string Endpoint => $"{Host}:{Port}";

    /// Разбор «1=127.0.0.1:6001,2=127.0.0.1:6002». Пусто — состав из пяти узлов на 6001…6005.
    public static List<Member> Parse(string? setting)
    {
        if (string.IsNullOrWhiteSpace(setting))
        {
            return Enumerable.Range(1, 5)
                .Select(id => new Member(id, "127.0.0.1", 6000 + id))
                .ToList();
        }

        var members = new List<Member>();

        foreach (string item in setting.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            string[] parts = item.Split('=', ':');

            if (parts.Length != 3 ||
                !int.TryParse(parts[0], out int id) ||
                !int.TryParse(parts[2], out int port))
            {
                throw new FormatException($"CLUSTER_MEMBERS: не разобран элемент '{item}', ожидается вид 1=127.0.0.1:6001");
            }

            members.Add(new Member(id, parts[1], port));
        }

        if (members.Count == 0)
        {
            throw new FormatException("CLUSTER_MEMBERS: список участников пуст");
        }

        if (members.Select(member => member.Id).Distinct().Count() != members.Count)
        {
            throw new FormatException($"CLUSTER_MEMBERS: id повторяются: {string.Join(", ", members.Select(member => member.Id))}");
        }

        return members.OrderBy(member => member.Id).ToList();
    }
}
