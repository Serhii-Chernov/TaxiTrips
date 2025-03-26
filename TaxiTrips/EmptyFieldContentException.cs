namespace TaxiTrips;

/// <summary>
/// Custom exception used to indicate that a CSV field is empty or null where not allowed.
/// </summary>
public class EmptyFieldContentException : Exception

{
    public EmptyFieldContentException() : base($"Empty field content.")
    {
    }
}