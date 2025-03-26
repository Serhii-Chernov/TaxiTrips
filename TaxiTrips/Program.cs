using System;
using Microsoft.Extensions.Configuration;

namespace TaxiTrips;

class Program
{
    static void Main()
    {
        try
        {
            // Build configuration from appsettings.json
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json") // Load configuration file
                .Build();

            // Read settings from configuration
            string csvPath = config["CsvPath"];
            string duplicatesCsvPath = config["DuplicatesCsvPath"];
            string logPath = config["LogPath"];
            string connectionString = config["ConnectionString"];

            // Create ETL process instance with provided settings
            ETLProcess etl = new ETLProcess(csvPath, duplicatesCsvPath, connectionString, logPath);

            // Start ETL process
            etl.Run();

            // Wait for user input before closing (for debug purposes)
            Console.ReadLine();
        }
        catch (Exception ex)
        {
            // Log any unexpected error
            Console.WriteLine(ex.ToString());
            Console.ReadLine();
        }
    }
}