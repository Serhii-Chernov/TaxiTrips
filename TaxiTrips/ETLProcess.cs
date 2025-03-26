using System.Data;

namespace TaxiTrips;
/// <summary>
/// Orchestrates the ETL (Extract, Transform, Load) process:
/// reads CSV data, transforms and validates it, then loads into the database.
/// </summary>
public class ETLProcess

{
    private readonly CsvReader _csvReader;
    private readonly CsvParser _csvParser;
    private readonly RowValidator _validator;
    private readonly SqlDataInserter _sqlInserter;
    private readonly Logger _logger;

    public ETLProcess(string csvPath, string duplicatesCsvPath, string connectionString, string logPath)
    {
        // Initialize components
        _csvReader = new CsvReader(csvPath);                  // Handles reading CSV file
        _csvParser = new CsvParser();                         // Parses and transforms each row
        _validator = new RowValidator();                      // Validates business logic on each row
        _sqlInserter = new SqlDataInserter(connectionString, duplicatesCsvPath); // Handles DB operations
        _logger = new Logger(logPath);                        // Logs messages and errors
    }

    public void Run()
    {
        try
        {
            DataTable dataTable = CreateTableSchema();  // Create structure for parsed data
            long totalRows = 0;

            // Read CSV rows and header mapping
            var (csvRows, headerMap) = _csvReader.ReadCsv();

            foreach (var fields in csvRows)
            {
                DataRow row;
                try
                {
                    // Parse raw CSV row into structured DataRow
                    row = _csvParser.ParseAndTransformRow(fields, headerMap, dataTable);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Parsing error: {ex.Message}\t fields: {string.Join(",", fields)}");
                    continue;
                }

                try
                {
                    // Validate row (e.g. check for invalid values)
                    _validator.Validate(row);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Failed to validate row: {ex.Message}\t fields: {string.Join(",", fields)}");
                    continue;
                }

                // Add valid row to batch
                dataTable.Rows.Add(row);
                totalRows++;

                // Insert batch if threshold is reached
                if (dataTable.Rows.Count >= 5000)
                {
                    try
                    {
                        _logger.LogInfo($"Inserting {dataTable.Rows.Count} rows into db");
                        _sqlInserter.BulkInsertAndDuplicate(dataTable);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError($"Error while inserting data table: {e}");
                    }
                    finally
                    {
                        dataTable.Clear();
                    }
                }
            }

            // Insert remaining rows
            if (dataTable.Rows.Count > 0)
            {
                try
                {
                    _logger.LogInfo($"Inserting {dataTable.Rows.Count} rows into db");
                    _sqlInserter.BulkInsertAndDuplicate(dataTable);
                }
                catch (Exception e)
                {
                    _logger.LogError($"Error while inserting data table: {e}");
                }
                finally
                {
                    dataTable.Clear();
                }
            }

            _logger.LogInfo($"ETL completed! Processed {totalRows} rows.");
        }
        catch (Exception e)
        {
            _logger.LogError($"Error occurred while processing: {e}");
        }
        finally
        {
            // Cleanup staging table after ETL is done
            _sqlInserter.Cleanup();
        }
    }

    private DataTable CreateTableSchema()
    {
        // Define schema for data table that matches DB table structure
        DataTable dt = new DataTable();
        dt.Columns.Add("PickupDateTime", typeof(DateTime));
        dt.Columns.Add("DropoffDateTime", typeof(DateTime));
        dt.Columns.Add("PassengerCount", typeof(int));
        dt.Columns.Add("TripDistance", typeof(decimal));
        dt.Columns.Add("StoreAndFwdFlag", typeof(string));
        dt.Columns.Add("PULocationID", typeof(int));
        dt.Columns.Add("DOLocationID", typeof(int));
        dt.Columns.Add("FareAmount", typeof(decimal));
        dt.Columns.Add("TipAmount", typeof(decimal));
        return dt;
    }
}
