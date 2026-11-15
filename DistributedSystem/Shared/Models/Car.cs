namespace Shared.Models;

public class Car
{
    public int Id { get; set; }
    public string Brand { get; set; } = string.Empty;
    public string Model { get; set; } = string.Empty;
    public string RegistrationNumber { get; set; } = string.Empty;
    public int LocationId { get; set; }
    public decimal DailyPrice { get; set; }
    public bool IsAvailable { get; set; }
}
