using Shared.Models;

namespace Shared.Protocol;

/// Состояние одного экземпляра данных: доступен ли он, какая версия данных в нём лежит
/// и какие значения у реплицируемого ресурса.
public class InstanceInfo
{
    public string Name { get; set; } = string.Empty;
    public string State { get; set; } = string.Empty;
    public long Lsn { get; set; }
    public List<Car> Cars { get; set; } = new();
    public int Rentals { get; set; }
    public bool IsActive { get; set; }
}
