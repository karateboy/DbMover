using Serilog;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbMover;
record EpaData(DateTime DateTime, int Monitor, string MonitorType, float Value);
internal class DbTask
{
    private readonly string localConnectionStr;
    private readonly List<EpaData> _data;

    public DbTask(string local, List<EpaData> data)
    {
        localConnectionStr = local;
        _data = data;
    }
    
    public async Task<int> UpsertDb()
    {
        int count = 0;
        foreach (var entry in _data)
        {
            count += await UpsertEntry(entry);
        }
        return count;
    }
    async Task<int> UpsertEntry(EpaData entry)
    {
        string queryStr = @"
            UPDATE [dbo].[hour_data]
                SET [MValue] = @value
            WHERE [MStation] = @m
                    and [MDate] = @dt
                    and [MItem] = @mt
            IF(@@ROWCOUNT = 0)
                BEGIN
                    INSERT INTO [dbo].[hour_data]
                    ([MStation]
                    ,[MDate]
                    ,[MItem]
                    ,[MValue])
                VALUES
                    (@m, @dt, @mt, @value)                    
                END";

        using (SqlConnection connection = new SqlConnection(localConnectionStr))
        {
            SqlCommand command = new SqlCommand(queryStr, connection);
            command.Parameters.AddWithValue("m", entry.Monitor);
            command.Parameters.AddWithValue("dt", entry.DateTime);
            command.Parameters.AddWithValue("mt", entry.MonitorType);
            command.Parameters.AddWithValue("value", entry.Value);
            command.Connection.Open();
            return await command.ExecuteNonQueryAsync();
        }
    }

    public BackgroundTask.BackgroundTaskGenerateDelegate MoveDbTaskGenerator()
    {
        return () => UpsertDb();
    }
}