namespace Shared.Protocol;

public class CreateRentalData
{
    public string FullName { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public string RegistrationNumber { get; set; } = string.Empty;
    public DateTime StartDate { get; set; }
    public int Days { get; set; }
    public bool SimulateError { get; set; }
    public string? ClientName { get; set; }
}
