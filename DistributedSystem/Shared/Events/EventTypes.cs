namespace Shared.Events;

/// <summary>
/// Типы событий варианта 11 «Аренда автомобилей». В работе тип события используется
/// как идентификатор подписки: Subscribe("RentalCreated").
/// </summary>
public static class EventTypes
{
    public const string RentalCreated = "RentalCreated";
    public const string RentalCancelled = "RentalCancelled";
    public const string CarReturned = "CarReturned";

    public static readonly string[] All = { RentalCreated, RentalCancelled, CarReturned };

    public static bool IsKnown(string? eventType) =>
        eventType != null && Array.IndexOf(All, eventType) >= 0;
}
