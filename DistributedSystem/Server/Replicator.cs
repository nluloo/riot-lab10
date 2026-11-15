using Npgsql;
using Shared.Models;

namespace Server;

/// <summary>
/// Передача изменений между копиями. После успешного изменения в активной копии сервер
/// переносит затронутые таблицы в вторую копию и переносит туда же версию данных (lsn).
/// </summary>
public class Replicator
{
    private readonly Journal _journal;

    public Replicator(Journal journal)
    {
        _journal = journal;
    }

    /// Перенести затронутые операции таблицы из source в target.
    /// Ошибки не глотаются: вызывающий код зарегистрирует неудачную репликацию.
    public int Replicate(DataInstance source, DataInstance target, IEnumerable<string> tables, string scope)
    {
        List<string> ordered = Order(tables);
        long lsn = source.Database.GetLsn();
        int copied = 0;

        _journal.Write(scope, $"{target.Name} -> UPDATE {string.Join(", ", ordered)}");

        using NpgsqlConnection connection = target.Database.Open();
        using NpgsqlTransaction transaction = connection.BeginTransaction();

        foreach (string table in ordered)
        {
            copied += CopyTable(table, source.Database, target.Database, connection, transaction);
        }

        target.Database.SetLsn(connection, transaction, lsn);
        target.Database.AlignSequences(connection, transaction);
        transaction.Commit();

        target.RememberLsn(lsn);
        _journal.Write(scope, $"Replication -> SUCCESS, {copied} строк передано в {target.Name} (lsn = {lsn})");

        return copied;
    }

    /// Полная синхронизация: перенести все реплицируемые таблицы. Используется для начальной
    /// сверки копий и для восстановления согласованности после отказа.
    public int FullSync(DataInstance source, DataInstance target, string scope)
    {
        long lsn = source.Database.GetLsn();
        int copied = 0;
        int deleted = 0;

        using NpgsqlConnection connection = target.Database.Open();
        using NpgsqlTransaction transaction = connection.BeginTransaction();

        foreach (string table in Order(CarRentalDatabase.ReplicatedTables).AsEnumerable().Reverse())
        {
            List<int> ids = ReadIds(table, source.Database);
            deleted += target.Database.DeleteMissing(connection, transaction, table, ids);
        }

        foreach (string table in Order(CarRentalDatabase.ReplicatedTables))
        {
            copied += CopyTable(table, source.Database, target.Database, connection, transaction);
        }

        target.Database.SetLsn(connection, transaction, lsn);
        target.Database.AlignSequences(connection, transaction);
        transaction.Commit();

        target.RememberLsn(lsn);
        _journal.Write(scope,
            $"Synchronization -> SUCCESS, перенесено {copied} строк из {source.Name} в {target.Name}, " +
            $"удалено {deleted}, lsn = {lsn}");

        return copied;
    }

    /// Сравнить содержимое двух копий. Различия считаются по строкам и по версии данных.
    public DiffReport Compare(DataInstance primary, DataInstance replica)
    {
        var report = new DiffReport
        {
            PrimaryLsn = primary.Database.GetLsn(),
            ReplicaLsn = replica.Database.GetLsn()
        };

        foreach (string table in CarRentalDatabase.ReplicatedTables)
        {
            Dictionary<int, string> left = primary.Database.GetRowFingerprints(table);
            Dictionary<int, string> right = replica.Database.GetRowFingerprints(table);

            foreach ((int id, string value) in left)
            {
                if (!right.TryGetValue(id, out string? rightValue))
                {
                    report.Differences.Add($"{table}: id {id} есть только в {primary.Name}");
                }
                else if (rightValue != value)
                {
                    report.Differences.Add($"{table}: id {id} различается");
                }
            }

            foreach (int id in right.Keys.Except(left.Keys))
            {
                report.Differences.Add($"{table}: id {id} есть только в {replica.Name}");
            }
        }

        if (report.PrimaryLsn != report.ReplicaLsn)
        {
            report.Differences.Add($"replication_meta: lsn различается ({report.PrimaryLsn} против {report.ReplicaLsn})");
        }

        return report;
    }

    private int CopyTable(
        string table,
        CarRentalDatabase source,
        CarRentalDatabase target,
        NpgsqlConnection connection,
        NpgsqlTransaction transaction)
    {
        switch (table)
        {
            case "locations":
            {
                List<Location> rows = source.GetLocations();
                target.UpsertLocations(connection, transaction, rows);
                target.DeleteMissing(connection, transaction, table, rows.Select(row => row.Id));
                return rows.Count;
            }
            case "cars":
            {
                List<Car> rows = source.GetCars();
                target.UpsertCars(connection, transaction, rows);
                target.DeleteMissing(connection, transaction, table, rows.Select(row => row.Id));
                return rows.Count;
            }
            case "customers":
            {
                List<Customer> rows = source.GetCustomers();
                target.UpsertCustomers(connection, transaction, rows);
                target.DeleteMissing(connection, transaction, table, rows.Select(row => row.Id));
                return rows.Count;
            }
            case "rentals":
            {
                List<Rental> rows = source.GetAllRentals();
                target.UpsertRentals(connection, transaction, rows);
                target.DeleteMissing(connection, transaction, table, rows.Select(row => row.Id));
                return rows.Count;
            }
            default:
                throw new InvalidOperationException($"Таблица {table} не реплицируется");
        }
    }

    private static List<int> ReadIds(string table, CarRentalDatabase source)
    {
        return table switch
        {
            "locations" => source.GetLocations().Select(row => row.Id).ToList(),
            "cars" => source.GetCars().Select(row => row.Id).ToList(),
            "customers" => source.GetCustomers().Select(row => row.Id).ToList(),
            "rentals" => source.GetAllRentals().Select(row => row.Id).ToList(),
            _ => throw new InvalidOperationException($"Таблица {table} не реплицируется")
        };
    }

    private static List<string> Order(IEnumerable<string> tables)
    {
        return tables
            .Distinct()
            .OrderBy(table => Array.IndexOf(CarRentalDatabase.ReplicatedTables, table))
            .ToList();
    }
}

public class DiffReport
{
    public long PrimaryLsn { get; set; }
    public long ReplicaLsn { get; set; }
    public List<string> Differences { get; } = new();

    public bool DataMatch => Differences.All(item => !item.StartsWith("replication_meta"));

    public bool IsConsistent => DataMatch && PrimaryLsn == ReplicaLsn;
}
