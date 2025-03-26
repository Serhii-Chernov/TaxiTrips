using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;


namespace TaxiTrips;

/// <summary>
/// Reads a CSV file and extracts rows along with a header-to-index mapping.
/// </summary>
public class CsvReader

{
    private readonly string _filePath;

    public CsvReader(string filePath)
    {
        _filePath = filePath;
    }

    public (List<string[]> Rows, Dictionary<string, int> HeaderMap) ReadCsv()
    {
        try
        {
            var rows = new List<string[]>();
            var headerMap = new Dictionary<string, int>();

            using var reader = new StreamReader(_filePath);
            using var csv = new CsvHelper.CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture));

            // Read header row and build a map of column names to indices
            csv.Read();
            csv.ReadHeader();

            var headerRow = csv.HeaderRecord;
            if (headerRow == null)
                throw new ApplicationException("Header row is empty");

            for (int i = 0; i < headerRow.Length; i++)
            {
                headerMap[headerRow[i]] = i;
            }

            // Read all data rows into memory
            while (csv.Read())
            {
                rows.Add(csv.Parser.Record);
            }

            return (rows, headerMap);
        }
        catch (Exception ex)
        {
            throw new ApplicationException("Error while reading file: " + _filePath, ex);
        }
    }
}