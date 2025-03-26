using System;
using System.Data;
using System.Globalization;

namespace TaxiTrips;

/// <summary>
/// Parses a raw CSV row (string array) into a typed DataRow with transformations,
/// including timezone conversion and value normalization.
/// </summary>
public class CsvParser

{
    public DataRow ParseAndTransformRow(string[] fields, Dictionary<string, int> headerMap, DataTable schema)
    {
        // Check for empty fields
        foreach (var field in fields)
        {
            if (string.IsNullOrEmpty(field))
                throw new EmptyFieldContentException();
        }

        // Map fields to DataTable row
        DataRow row = schema.NewRow();
        row["PickupDateTime"] = ParseDateTime(fields[headerMap["tpep_pickup_datetime"]]);
        row["DropoffDateTime"] = ParseDateTime(fields[headerMap["tpep_dropoff_datetime"]]);
        row["PassengerCount"] = int.Parse(fields[headerMap["passenger_count"]]);
        row["TripDistance"] = decimal.Parse(fields[headerMap["trip_distance"]], CultureInfo.InvariantCulture);
        row["StoreAndFwdFlag"] = ParseStoreAndFwdFlag(fields[headerMap["store_and_fwd_flag"]]);
        row["PULocationID"] = int.Parse(fields[headerMap["PULocationID"]]);
        row["DOLocationID"] = int.Parse(fields[headerMap["DOLocationID"]]);
        row["FareAmount"] = decimal.Parse(fields[headerMap["fare_amount"]], CultureInfo.InvariantCulture);
        row["TipAmount"] = decimal.Parse(fields[headerMap["tip_amount"]], CultureInfo.InvariantCulture);

        return row;
    }

    private DateTime ParseDateTime(string estTime)
    {
        // Parse string to DateTime and convert from EST to UTC
        DateTime parsedTime = DateTime.Parse(estTime, CultureInfo.InvariantCulture);
        TimeZoneInfo est = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
        return TimeZoneInfo.ConvertTimeToUtc(parsedTime, est);
    }

    private string ParseStoreAndFwdFlag(string storeAndFwdFlag)
    {
        // Convert Y/N to Yes/No
        if (storeAndFwdFlag == "Y")
            return "Yes";
        if (storeAndFwdFlag == "N")
            return "No";

        throw new ApplicationException($"Invalid StoreAndFwdFlag: {storeAndFwdFlag}");
    }
}
