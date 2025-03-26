namespace TaxiTrips;

/// <summary>
/// Logs messages of various severity levels (info, warning, error) to both file and console.
/// </summary>
public class Logger

{
    private readonly string _logFilePath;

    public Logger(string logFilePath)
    {
        _logFilePath = logFilePath;

        if (File.Exists(_logFilePath))
            File.Delete(_logFilePath);
    }

    // Log an error message
    public void LogError(string message)
    {
        string logEntry = $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} | ERROR: {message}";
        File.AppendAllText(_logFilePath, logEntry + Environment.NewLine);
        Console.WriteLine($"ERROR: {message}");
    }

    // Log an informational message
    public void LogInfo(string message)
    {
        string logEntry = $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} | INFO: {message}";
        File.AppendAllText(_logFilePath, logEntry + Environment.NewLine);
        Console.WriteLine($"INFO: {message}");
    }

    // Log a warning message
    public void LogWarning(string message)
    {
        string logEntry = $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} | WARN: {message}";
        File.AppendAllText(_logFilePath, logEntry + Environment.NewLine);
        Console.WriteLine($"WARN: {message}");
    }
}
