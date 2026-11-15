using Npgsql;

namespace Server;

/// <summary>
/// Основную и резервную копию данных выбирает InstanceManager: он хранит состояние каждого экземпляра, проверяет доступность и переключает запрос на доступную копию (failover).
/// </summary>
public class InstanceManager
{
    private readonly Journal _journal;
    private readonly object _sync = new();

    public InstanceManager(DataInstance primary, DataInstance replica, Journal journal)
    {
        Primary = primary;
        Replica = replica;
        _journal = journal;
    }

    public DataInstance Primary { get; }

    public DataInstance Replica { get; }

    public DataInstance? Active { get; private set; }

    public DataInstance Peer(DataInstance instance) =>
        ReferenceEquals(instance, Primary) ? Replica : Primary;

    public DataInstance? SelectForRequest(string scope)
    {
        lock (_sync)
        {
            _journal.Write(scope, "Primary -> CHECK");
            bool primaryUp = Primary.Refresh();
            _journal.Write(scope, $"Primary -> {Primary.StatusText} (lsn = {Primary.Lsn})");

            if (!primaryUp)
            {
                _journal.Write(scope, "Failover -> START");
            }

            _journal.Write(scope, "Replica -> CHECK");
            bool replicaUp = Replica.Refresh();
            _journal.Write(scope, $"Replica -> {Replica.StatusText} (lsn = {Replica.Lsn})");

            if (!primaryUp && !replicaUp)
            {
                _journal.Write(scope, "Failover -> FAIL, оба экземпляра недоступны");
                Active = null;
                return null;
            }

            DataInstance chosen = Choose(primaryUp, replicaUp);

            if (!ReferenceEquals(chosen, Active))
            {
                if (Active == null)
                {
                    _journal.Write(scope, $"Request -> через {chosen.Name}");
                }
                else if (ReferenceEquals(chosen, Replica))
                {
                    _journal.Write(scope, "Failover -> SUCCESS, запрос идёт через Replica");
                }
                else
                {
                    _journal.Write(scope, "Failover -> OFF, запрос снова идёт через Primary");
                }

                Active = chosen;
            }

            return Active;
        }
    }

    /// Фоновый контроль состояния: замечает отказ и восстановление экземпляра до запроса клиента.
    public async Task MonitorAsync(int intervalMs, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                RefreshStates();
            }
            catch (Exception ex)
            {
                _journal.Write("Heartbeat", $"Ошибка проверки состояния: {ex.Message}");
            }

            try
            {
                await Task.Delay(intervalMs, token);
            }
            catch (TaskCanceledException)
            {
                break;
            }
        }
    }

    internal void RefreshStates()
    {
        lock (_sync)
        {
            foreach (DataInstance instance in new[] { Primary, Replica })
            {
                bool wasAvailable = instance.IsAvailable;
                long lsnBefore = instance.Lsn;

                instance.Refresh();

                if (wasAvailable != instance.IsAvailable)
                {
                    _journal.Write("Heartbeat", $"{instance.Name} -> {instance.StatusText} (lsn = {instance.Lsn})");
                }
                else if (instance.IsAvailable && lsnBefore != instance.Lsn)
                {
                    _journal.Write("Heartbeat", $"{instance.Name} -> данные изменились (lsn = {instance.Lsn})");
                }

                if (!wasAvailable && instance.IsAvailable)
                {
                    PrepareSchema(instance);
                    ReportDivergence(instance);
                }
            }
        }
    }

    /// База могла быть недоступна в момент старта сервера, и тогда схема не была создана:
    /// при восстановлении экземпляра создаём её повторно. EnsureCreated идемпотентен
    /// (CREATE TABLE IF NOT EXISTS), поэтому здесь нет отдельного SQL.
    private void PrepareSchema(DataInstance instance)
    {
        try
        {
            instance.Database.EnsureCreated();
            _journal.Write("Heartbeat", $"{instance.Name} -> SCHEMA READY после восстановления");
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
        {
            // Мониторинг не должен падать из-за схемы: её создадим при следующей проверке.
            _journal.Write("Heartbeat", $"{instance.Name} -> не удалось подготовить схему: {ex.Message}");
        }
    }

    private void ReportDivergence(DataInstance recovered)
    {
        DataInstance peer = Peer(recovered);

        if (!peer.IsAvailable || recovered.Lsn == peer.Lsn)
        {
            return;
        }

        DataInstance newest = recovered.Lsn > peer.Lsn ? recovered : peer;
        _journal.Write("Recovery",
            $"{recovered.Name} восстановлен, копии различаются: " +
            $"Primary lsn = {Primary.Lsn}, Replica lsn = {Replica.Lsn}. " +
            $"Актуальные данные в {newest.Name}, нужна синхронизация");
    }

    private DataInstance Choose(bool primaryUp, bool replicaUp)
    {
        if (primaryUp && replicaUp)
        {
            // Replica впереди по версии значит, что Primary восстановился после отказа
            // и ещё не синхронизирован: устаревшую копию для запросов не используем.
            if (Replica.Lsn > Primary.Lsn)
            {
                return Replica;
            }

            return Primary;
        }

        return primaryUp ? Primary : Replica;
    }
}
