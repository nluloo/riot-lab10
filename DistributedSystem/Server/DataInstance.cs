using Npgsql;

namespace Server;

public enum InstanceStatus
{
    Unavailable,
    Available
}

/// <summary>
/// Один экземпляр хранения данных: база PostgreSQL, её состояние и последняя известная версия данных.
/// </summary>
public class DataInstance
{
    public DataInstance(string name, CarRentalDatabase database)
    {
        Name = name;
        Database = database;
    }

    public string Name { get; }

    public CarRentalDatabase Database { get; }

    public InstanceStatus Status { get; private set; } = InstanceStatus.Available;

    /// Версия данных экземпляра. Читается из таблицы replication_meta при проверке состояния.
    public long Lsn { get; private set; }

    public DateTime LastCheckedAt { get; private set; }

    public string StatusText => Status.ToString().ToUpperInvariant();

    public bool IsAvailable => Status == InstanceStatus.Available;

    /// Проверить доступность и прочитать текущую версию данных.
    public bool Refresh()
    {
        bool available = Database.Ping();

        Status = available ? InstanceStatus.Available : InstanceStatus.Unavailable;
        LastCheckedAt = DateTime.Now;

        if (available)
        {
            try
            {
                Lsn = Database.GetLsn();
            }
            catch (NpgsqlException)
            {
                // Подключение сорвалось на чтении версии: экземпляр считается недоступным.
                // Такое бывает в момент старта контейнера, когда база ещё не готова.
                Status = InstanceStatus.Unavailable;
                available = false;
            }
        }

        // Если экземпляр недоступен, последняя известная версия остаётся: по ней видно отставание.
        return available;
    }

    /// Запомнить версию данных сразу после переноса изменений в этот экземпляр.
    internal void RememberLsn(long lsn) => Lsn = lsn;
}
