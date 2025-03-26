using System.Data;

namespace TaxiTrips;

/// <summary>
/// Validates a DataRow against business rules such as date order, non-negative values, etc.
/// </summary>
public class RowValidator

{
    public void Validate(DataRow row)
    {
        // Basic business rules
        if ((DateTime)row["PickupDateTime"] > (DateTime)row["DropoffDateTime"])
            throw new ApplicationException("Pickup date is later than dropoff date.");

        if ((int)row["PassengerCount"] < 0)
            throw new ApplicationException("Passenger count is negative.");

        if ((decimal)row["FareAmount"] < 0)
            throw new ApplicationException("Fare amount is negative.");

        if ((decimal)row["TipAmount"] < 0)
            throw new ApplicationException("Tip amount is negative.");
    }
}
