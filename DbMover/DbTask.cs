using Serilog;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbMover;

internal class DbTask
{
    private readonly string localConnectionStr;
    private readonly string remoteConnectionStr;

    public DbTask(string local, string remote)
    {
        localConnectionStr = local;
        remoteConnectionStr = remote;
    }

    async Task<Dictionary<DateTime, float>> GetGenerating(DateTime start)
    {
        string queryStr = @"SELECT
        [PointSliceID]
        ,[UTCDateTime]
        ,[ActualValue]
        FROM [JCIHistorianDB].[dbo].[tblActualValueFloat]
        Where PointSliceID in (624, 626, 628, 630, 632, 634, 636, 638) and 
            [UTCDateTime] >= @start and [UTCDateTime] < @end
        Order by UTCDateTime desc";

        using (SqlConnection connection = new SqlConnection(remoteConnectionStr))
        {
            SqlCommand command = new SqlCommand(queryStr, connection);
            command.Parameters.AddWithValue("start", start.AddHours(-8));
            command.Parameters.AddWithValue("end", start.AddHours(17));
            command.Connection.Open();
            Dictionary<int, Dictionary<DateTime, float>> pointTimeValueMap = new();
            var reader = await command.ExecuteReaderAsync();
            while (reader.Read())
            {
                int point = (int)reader["PointSliceID"];
                DateTime dt = (DateTime)reader["UTCDateTime"];
                float value = (float)reader["ActualValue"];
                if (!pointTimeValueMap.ContainsKey(point))
                    pointTimeValueMap[point] = new();

                pointTimeValueMap[point][dt.AddHours(8)] = value;
            }

            IEnumerable<List<(DateTime, float)>> pointGeneratingEnumerator = pointTimeValueMap.Select(kvp =>
            {
                List<(DateTime dt, float Value)> totalList = kvp.Value.Select(kvp => (kvp.Key, kvp.Value))
                    .OrderByDescending(t => t.Key).ToList();

                List<(DateTime, float)> generatingList = new();
                while (totalList.Count >= 2)
                {
                    (DateTime dt, float Value) endRecord = totalList[0];
                    (DateTime dt, float Value) startRecord = totalList[1];

                    if (startRecord.dt.Hour <= 5 || startRecord.dt.Hour >= 19)
                    {
                        generatingList.Add((startRecord.dt, 0));
                    }
                    else
                    {
                        float generatedKWHour = Math.Abs(endRecord.Value - startRecord.Value);
                        TimeSpan ts = endRecord.dt - startRecord.dt;
                        float generatedKW = (float)(generatedKWHour / ts.TotalMinutes * 60);

                        generatingList.Add((startRecord.dt, Math.Abs(generatedKW)));
                    }

                    totalList.RemoveAt(0);
                }

                return generatingList;
            });
            Dictionary<DateTime, float> totalGeneratingMap = new();
            foreach (List<(DateTime, float)> generatingList in pointGeneratingEnumerator)
            {
                foreach (var generating in generatingList)
                {
                    if (!totalGeneratingMap.ContainsKey(generating.Item1))
                        totalGeneratingMap[generating.Item1] = 0;

                    totalGeneratingMap[generating.Item1] += generating.Item2;
                }
            }

            return totalGeneratingMap;
        }
    }

    async Task<Dictionary<DateTime, float>> GetConsuming(DateTime start)
    {
        string queryStr = @"SELECT 
            [PointSliceID],[UTCDateTime],[ActualValue]
            FROM [JCIHistorianDB].[dbo].[tblActualValueFloat]
            Where PointSliceID = 239 and [UTCDateTime] >= @start and [UTCDateTime] < @end
            Order by UTCDateTime desc";

        using (SqlConnection connection = new SqlConnection(remoteConnectionStr))
        {
            SqlCommand command = new SqlCommand(queryStr, connection);
            command.Parameters.AddWithValue("start", start.AddHours(-8));
            command.Parameters.AddWithValue("end", start.AddHours(17));
            command.Connection.Open();
            Dictionary<DateTime, float> consumingMap = new Dictionary<DateTime, float>();
            var reader = await command.ExecuteReaderAsync();
            while (reader.Read())
            {
                DateTime dt = (DateTime)reader["UTCDateTime"];
                float value = (float)reader["ActualValue"];
                consumingMap[dt.AddHours(8)] = value;
            }

            return consumingMap;
        }
    }

    async Task<int> UpsertMinRecord(DateTime dt, double generating, double consuming)
    {
        string queryStr = @"
            UPDATE [dbo].[MinRecord]
                SET [MonitorID] = 3
                ,[DateTime] = @dt
                ,[generating] = @generating
                ,[storing] = null
                ,[consuming] = @consuming
            WHERE [MonitorID] = 3 and [DateTime] = @dt
            IF(@@ROWCOUNT = 0)
                BEGIN
                    INSERT INTO [dbo].[MinRecord]
                    ([MonitorID], [DateTime], [generating], [storing], [consuming])
                    VALUES
                    (3, @dt, @generating, null,@consuming)
                END";

        using (SqlConnection connection = new SqlConnection(localConnectionStr))
        {
            SqlCommand command = new SqlCommand(queryStr, connection);
            command.Parameters.AddWithValue("generating", generating > 2000 ? 2000 : generating);
            command.Parameters.AddWithValue("consuming", consuming > 3382 ? 3382 : consuming);
            command.Parameters.AddWithValue("dt", dt);
            command.Connection.Open();
            return await command.ExecuteNonQueryAsync();
        }
    }

    async Task<int> UpsertHourRecord(DateTime dt, double generating, double consuming)
    {
        string queryStr = @"
            UPDATE [dbo].[HourRecord]
                SET [MonitorID] = 3
                ,[DateTime] = @dt
                ,[generating] = @generating
                ,[storing] = null
                ,[consuming] = @consuming
            WHERE [MonitorID] = 3 and [DateTime] = @dt
            IF(@@ROWCOUNT = 0)
                BEGIN
                    INSERT INTO [dbo].[HourRecord]
                    ([MonitorID], [DateTime], [generating], [storing], [consuming])
                    VALUES
                    (3, @dt, @generating, null,@consuming)
                END";
        ;

        using (SqlConnection connection = new SqlConnection(localConnectionStr))
        {
            SqlCommand command = new SqlCommand(queryStr, connection);
            command.Parameters.AddWithValue("generating", generating > 2000 ? 2000 : generating);
            command.Parameters.AddWithValue("consuming", consuming > 3382 ? 3382 : consuming);
            command.Parameters.AddWithValue("dt", dt);
            command.Connection.Open();
            return await command.ExecuteNonQueryAsync();
        }
    }

    public async Task GrabRemoteDbAt(DateTime now)
    {
        try
        {
            DateTime start = now.AddHours(-now.Hour).AddMinutes(-now.Minute)
                .AddSeconds(-now.Second).AddMilliseconds(-now.Millisecond);
            var generatingMap = await GetGenerating(start);
            var consumingMap = await GetConsuming(start);
            Log.Information("Get data complete generating: {0} consuming: {1}", generatingMap.Count,
                consumingMap.Count);

            List<(DateTime, float, float)> updateList = new();
            foreach (var dt in generatingMap.Keys)
            {
                var generating = generatingMap[dt] > 2000 ? 2000 : generatingMap[dt];
                var consuming = consumingMap.TryGetValue(dt, out var value) ? 
                    (value > 3328 ? 3328 : value) : 0;
                updateList.Add((dt, generating, consuming));
            }

            updateList.Sort((a, b) => a.Item1.CompareTo(b.Item1));
            if (updateList.Count > 0)
            {
                foreach (var update in updateList)
                {
                    await UpsertMinRecord(update.Item1, update.Item2, update.Item3);
                }

                var minDt = updateList.First();
                var maxDt = updateList.Last();

                Log.Information($"{minDt.Item1:g} => {maxDt.Item1:g}: total {updateList.Count} min upserted");
                for (int i = 0; i < 24; i++)
                {
                    DateTime hourStart = start.AddHours(i);
                    DateTime hourEnd = hourStart.AddHours(1);
                    if (hourEnd >= DateTime.Now)
                        continue;

                    var hourList = updateList.Where(t => t.Item1 >= hourStart && t.Item1 < hourEnd).ToList();
                    if (hourList.Count > 0)
                    {
                        double generatingAvg = hourList.Select(t => t.Item2).Average();
                        double consumingAvg = hourList.Select(t => t.Item3).Average();
                        await UpsertHourRecord(hourStart, generatingAvg, consumingAvg);
                    }
                    else
                    {
                        await UpsertHourRecord(hourStart, 0, 0);
                    }
                }

                Log.Information($"{start:g} hourRecord upserted");
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex, "failed");
        }

        return;
    }

    public BackgroundTask.BackgroundTaskGenerateDelegate MoveDbTaskGenerator()
    {
        return () => GrabRemoteDbAt(DateTime.Now.AddDays(-1));
    }
}