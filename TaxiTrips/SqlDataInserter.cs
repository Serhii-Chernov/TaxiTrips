using System;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace TaxiTrips;

/// <summary>
/// Handles bulk insertion of transformed data into a SQL Server database,
/// including duplicate detection, exporting to file, and cleanup of temporary tables.
/// </summary>
public class SqlDataInserter

{
    private readonly string _connectionString;
    private readonly string _duplicatesFilePath;
    private const string StagingTableName = "Staging_TaxiTrips";

    public SqlDataInserter(string connectionString, string duplicatesFilePath)
    {
        _connectionString = connectionString;
        _duplicatesFilePath = duplicatesFilePath;
        Prepare(); // Prepare database and duplicate file
    }

    private void Prepare()
    {
        PrepareDuplicatesFile(); // Create empty duplicates.csv
        PrepareDb(); // Create staging table in DB
    }

    private void PrepareDuplicatesFile()
    {
        // Remove old file if exists, and write header row
        if (File.Exists(_duplicatesFilePath))
            File.Delete(_duplicatesFilePath);

        File.WriteAllText(_duplicatesFilePath,
            "PickupDateTime,DropoffDateTime,PassengerCount,TripDistance,StoreAndFwdFlag,PULocationID,DOLocationID,FareAmount,TipAmount\n");
    }

    private void PrepareDb()
    {
        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        // Drop and recreate the staging table
        var stagingTableCmd = @"
            IF OBJECT_ID('dbo.Staging_TaxiTrips', 'U') IS NOT NULL
                DROP TABLE dbo.Staging_TaxiTrips;

            CREATE TABLE dbo.Staging_TaxiTrips (
                Id               BIGINT IDENTITY(1,1) PRIMARY KEY,
                PickupDateTime   DATETIME2     NOT NULL,
                DropoffDateTime  DATETIME2     NOT NULL,
                PassengerCount   INT           NOT NULL,
                TripDistance     DECIMAL(9,3)  NOT NULL,
                StoreAndFwdFlag  VARCHAR(3)    NOT NULL,
                PULocationID     INT           NOT NULL,
                DOLocationID     INT           NOT NULL,
                FareAmount       DECIMAL(10,2) NOT NULL,
                TipAmount        DECIMAL(10,2) NOT NULL
            );";

        using var stagingCmd = new SqlCommand(stagingTableCmd, conn);
        stagingCmd.ExecuteNonQuery();

        // Optional: truncate main table for testing
        var truncateCmd = "TRUNCATE TABLE TaxiTrips;";
        using var cmdTruncate = new SqlCommand(truncateCmd, conn);
        cmdTruncate.ExecuteNonQuery();
    }

    public void Cleanup()
    {
        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        // Drop staging table after process
        using var cmd = new SqlCommand("DROP TABLE IF EXISTS dbo.Staging_TaxiTrips;", conn);
        cmd.ExecuteNonQuery();
    }

    public void BulkInsertAndDuplicate(DataTable dataTable)
    {
        if (dataTable.Rows.Count == 0) return;

        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        // Step 1: Bulk insert into staging table
        using (var bulkCopy = new SqlBulkCopy(conn))
        {
            bulkCopy.DestinationTableName = StagingTableName;

            // Map DataTable columns to database columns
            bulkCopy.ColumnMappings.Add("PickupDateTime", "PickupDateTime");
            bulkCopy.ColumnMappings.Add("DropoffDateTime", "DropoffDateTime");
            bulkCopy.ColumnMappings.Add("PassengerCount", "PassengerCount");
            bulkCopy.ColumnMappings.Add("TripDistance", "TripDistance");
            bulkCopy.ColumnMappings.Add("StoreAndFwdFlag", "StoreAndFwdFlag");
            bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
            bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
            bulkCopy.ColumnMappings.Add("FareAmount", "FareAmount");
            bulkCopy.ColumnMappings.Add("TipAmount", "TipAmount");

            bulkCopy.WriteToServer(dataTable);
        }
        
        //Step 2: Save detected duplicates to CSV and remove duplicates from staging 
        DeleteAndExportDuplicates(conn);

        // Step 3: Move clean rows to main table
        using var cmd = new SqlCommand(@"
            INSERT INTO TaxiTrips
            (PickupDateTime, DropoffDateTime, PassengerCount, TripDistance, 
             StoreAndFwdFlag, PULocationID, DOLocationID, FareAmount, TipAmount)
            SELECT PickupDateTime, DropoffDateTime, PassengerCount, TripDistance,
                   StoreAndFwdFlag, PULocationID, DOLocationID, FareAmount, TipAmount
            FROM Staging_TaxiTrips;
            TRUNCATE TABLE Staging_TaxiTrips;", conn);
        cmd.ExecuteNonQuery();
    }

    private void DeleteAndExportDuplicates(SqlConnection conn)
    {
        // 1) Find duplicates inside staging (rn>1), export them
        var stagingDupRows = FindStagingDuplicates(conn);
        AppendToCsv(stagingDupRows);

        // 2) Delete duplicates inside staging
        DeleteStagingDuplicates(conn);

        // 3) Find duplicates with main table, export them
        var mainDupRows = FindDuplicatesWithMain(conn);
        AppendToCsv(mainDupRows);

        // 4) Delete duplicates with main table
        DeleteDuplicatesWithMain(conn);
    }

    #region Step1: find staging duplicates

    private IEnumerable<string> FindStagingDuplicates(SqlConnection conn)
    {
        const string query = @"
;WITH Numbered AS (
    SELECT s.*,
           ROW_NUMBER() OVER (
               PARTITION BY s.PickupDateTime, s.DropoffDateTime, s.PassengerCount
               ORDER BY (SELECT NULL)
           ) AS rn
    FROM Staging_TaxiTrips s
)
SELECT s.*
FROM Staging_TaxiTrips s
JOIN Numbered n ON s.Id = n.Id
WHERE n.rn > 1;
";

        using var cmd = new SqlCommand(query, conn);
        using var reader = cmd.ExecuteReader();

        return ReadRowsAsCsvLines(reader);
    }

    #endregion

    #region Step2: delete staging duplicates

    private void DeleteStagingDuplicates(SqlConnection conn)
    {
        const string deleteQuery = @"
;WITH Numbered AS (
    SELECT s.*,
           ROW_NUMBER() OVER (
               PARTITION BY s.PickupDateTime, s.DropoffDateTime, s.PassengerCount
               ORDER BY (SELECT NULL)
           ) AS rn
    FROM Staging_TaxiTrips s
)
DELETE s
FROM Staging_TaxiTrips s
JOIN Numbered n ON s.Id = n.Id
WHERE n.rn > 1;
";
        using var deleteCmd = new SqlCommand(deleteQuery, conn);
        deleteCmd.ExecuteNonQuery();
    }

    #endregion

    #region Step3: find duplicates with main

    private IEnumerable<string> FindDuplicatesWithMain(SqlConnection conn)
    {
        const string query = @"
SELECT s.*
FROM Staging_TaxiTrips s
JOIN TaxiTrips t
   ON  s.PickupDateTime  = t.PickupDateTime
   AND s.DropoffDateTime = t.DropoffDateTime
   AND s.PassengerCount  = t.PassengerCount;
";

        using var cmd = new SqlCommand(query, conn);
        using var reader = cmd.ExecuteReader();

        return ReadRowsAsCsvLines(reader);
    }

    #endregion

    #region Step4: delete duplicates with main

    private void DeleteDuplicatesWithMain(SqlConnection conn)
    {
        const string deleteQuery = @"
DELETE s
FROM Staging_TaxiTrips s
JOIN TaxiTrips t
   ON  s.PickupDateTime  = t.PickupDateTime
   AND s.DropoffDateTime = t.DropoffDateTime
   AND s.PassengerCount  = t.PassengerCount;
";
        using var deleteCmd = new SqlCommand(deleteQuery, conn);
        deleteCmd.ExecuteNonQuery();
    }

    #endregion

    #region Helper methods

    /// <summary>
    /// Reads all rows from a SqlDataReader and returns them as CSV strings
    /// </summary>
    private IEnumerable<string> ReadRowsAsCsvLines(SqlDataReader reader)
    {
        var list = new List<string>();

        while (reader.Read())
        {
            var row = new object[reader.FieldCount];
            reader.GetValues(row);
            
            for (int i = 0; i < row.Length; i++)
            {
                row[i] = row[i]?.ToString()?.Replace(",", ".");
            }

            list.Add(string.Join(",", row));
        }

        return list;
    }

    /// <summary>
    /// Appends lines to a file duplicates.csv
    /// </summary>
    private void AppendToCsv(IEnumerable<string> csvLines)
    {
        // Если строк нет, выходим
        if (csvLines == null) return;

        var sb = new StringBuilder();
        foreach (var line in csvLines)
        {
            sb.AppendLine(line);
        }

        File.AppendAllText(_duplicatesFilePath, sb.ToString());
    }

    #endregion


    // SQL fragments for identifying duplicates
    private const string DuplicatesInStagesCte = @"
DuplicatesInStaging AS (
    SELECT PickupDateTime, DropoffDateTime, PassengerCount, TripDistance,
           StoreAndFwdFlag, PULocationID, DOLocationID, FareAmount, TipAmount
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY PickupDateTime, DropoffDateTime, PassengerCount
            ORDER BY (SELECT NULL)
        ) AS rn
        FROM Staging_TaxiTrips
    ) t
    WHERE rn > 1
)";

    private const string DuplicatesWithMainCte = @"
DuplicatesWithMain AS (
    SELECT s.PickupDateTime, s.DropoffDateTime, s.PassengerCount, s.TripDistance,
           s.StoreAndFwdFlag, s.PULocationID, s.DOLocationID, s.FareAmount, s.TipAmount
    FROM Staging_TaxiTrips s
    JOIN TaxiTrips t
      ON s.PickupDateTime = t.PickupDateTime
     AND s.DropoffDateTime = t.DropoffDateTime
     AND s.PassengerCount = t.PassengerCount
)";
}