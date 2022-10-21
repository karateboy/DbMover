// See https://aka.ms/new-console-template for more information
using Serilog;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;

var cliArgs = Environment.GetCommandLineArgs();
Console.WriteLine($"Args = {string.Join(",", cliArgs)}");
SqlConnectionStringBuilder localBuilder = new SqlConnectionStringBuilder(@"Server=localhost\SQLEXPRESS;Database=slcems;Trusted_Connection=True;");
//setup local DB
localBuilder["Server"] = cliArgs[1];
Console.WriteLine(localBuilder.ConnectionString);
//setup remove DB
SqlConnectionStringBuilder remoteBuilder = new SqlConnectionStringBuilder(@"Server=localhost\SQLEXPRESS;Database=JCIHistorianDB;");
remoteBuilder["Server"] = cliArgs[2];
remoteBuilder["User"] = cliArgs[3];
remoteBuilder["Password"] = cliArgs[4];
Console.WriteLine(remoteBuilder.ConnectionString);


Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()                
                .WriteTo.File(".\\log\\DbMover.log", rollingInterval: RollingInterval.Day)
                .CreateLogger();

async Task<float?> GetGenerating()
{
    string queryStr = @"SELECT TOP 1
            [PointSliceID]
            ,[UTCDateTime]
            ,[OtherValue]
            FROM [JCIHistorianDB].[dbo].[tblOtherValueFloat]
            Where PointSliceID in (627, 629, 631, 633, 635, 637, 639) and UTCDateTime>= @start and UTCDateTime <@end
            Order by UTCDateTime desc";

    using (SqlConnection connection = new SqlConnection(remoteBuilder.ConnectionString))
    {
        SqlCommand command = new SqlCommand(queryStr, connection);
        command.Parameters.AddWithValue("start", DateTime.Now.AddHours(-1));
        command.Parameters.AddWithValue("end", DateTime.Now);
        command.Connection.Open();
        var reader = await command.ExecuteReaderAsync();
        while (reader.Read())
        {
            if (reader["OtherValue"] != DBNull.Value)
                return (float)reader["OtherValue"];

            return null;
        }
        return null;
    }
}

async Task<float?> GetConsuming()
{
    string queryStr = @"SELECT TOP 1
            [PointSliceID]
            ,[UTCDateTime]
            ,[OtherValue]
            FROM [JCIHistorianDB].[dbo].[tblOtherValueFloat]
            Where PointSliceID = 239 and UTCDateTime>= @start and UTCDateTime <@end
            Order by UTCDateTime desc";

    using (SqlConnection connection = new SqlConnection(remoteBuilder.ConnectionString))
    {
        SqlCommand command = new SqlCommand(queryStr, connection);
        command.Parameters.AddWithValue("start", DateTime.Now.AddHours(-1));
        command.Parameters.AddWithValue("end", DateTime.Now);
        command.Connection.Open();
        var reader = await command.ExecuteReaderAsync();
        while (reader.Read())
        {
            if (reader["OtherValue"] != DBNull.Value)
                return (float)reader["OtherValue"];

            return null;
        }
        return null;
    }
}

async Task<int> UpdateMinRecord(float? generating, float? consuming)
{
    string queryStr = @"INSERT INTO [dbo].[MinRecord]
           ([MonitorID]
           ,[DateTime]
           ,[generating]
           ,[storing]
           ,[consuming])
     VALUES
           (3
           ,@now
           ,@generating
           ,null
           ,@consuming)";

    using (SqlConnection connection = new SqlConnection(localBuilder.ConnectionString))
    {
        SqlCommand command = new SqlCommand(queryStr, connection);
        command.Parameters.AddWithValue("generating", generating.GetValueOrDefault(0));       
        command.Parameters.AddWithValue("consuming", consuming.GetValueOrDefault(0));
        var now = DateTime.Now;
        command.Parameters.AddWithValue("now", now.AddSeconds(-now.Second).AddMilliseconds(-now.Millisecond));
        command.Connection.Open();
        return await command.ExecuteNonQueryAsync();        
    }
}

async void GrabMinData()
{
    try
    {
        var generating = await GetGenerating();
        var consuming = await GetConsuming();
        Console.WriteLine($"{generating} - {consuming}");
        int affected = await UpdateMinRecord(generating, consuming);
        Console.WriteLine($"Insert {affected} min records");
    }
    catch (Exception ex)
    {
        Log.Error(ex, "move failed...");
    }
}

System.Timers.Timer minTimer;

void SetupMinTimer()
{
    DateTime now = DateTime.Now;
    TimeSpan ts = DateTime.Now.AddMinutes(1).AddSeconds(-now.Second + 30).AddMilliseconds(-now.Millisecond) - now;
    Log.Information($"set up 1 min timer");
    minTimer = new System.Timers.Timer(ts.TotalMilliseconds);
    minTimer.Elapsed += (sender, e) =>
    {
        DateTime currnt = DateTime.Now;
        TimeSpan nextTimeSpan = currnt.AddMinutes(1).AddMilliseconds(-currnt.Millisecond) - DateTime.Now;
        minTimer.Interval = nextTimeSpan.TotalMilliseconds;
        DateTime dtNow = DateTime.Now;
        DateTime adjustedNow = DateTime.Now.Date.AddMinutes((int)dtNow.TimeOfDay.TotalMinutes).AddMinutes(-2);
        GrabMinData();

        minTimer.Start();
    };
    minTimer.Start();
}

SetupMinTimer();

Console.WriteLine("Type exit to exit");
while (Console.ReadLine() != "exit");

minTimer.Stop();
