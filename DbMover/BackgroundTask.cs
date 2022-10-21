using Serilog;
using System.Data.SqlClient;

namespace DbMover;

internal class BackgroundTask
{
    private Task? _timerTask;
    private readonly PeriodicTimer _timer;
    private readonly CancellationTokenSource _cts = new();
    private readonly string localConnectionStr;
    private readonly string remoteConnectionStr;
    public BackgroundTask(string local, string remote, TimeSpan inverval)
    {
        localConnectionStr = local;
        remoteConnectionStr = remote;
        _timer = new PeriodicTimer(inverval);
    }
    async public     Task
Start()
    {
        await ActualWork();
        _timerTask = DoWorkAsync();
    }

    async Task<Dictionary<DateTime, float>> GetGenerating()
    {
        string queryStr = @"SELECT TOP 1152 
        [PointSliceID]
        ,[UTCDateTime]
        ,[ActualValue]
        FROM [JCIHistorianDB].[dbo].[tblActualValueFloat]
        Where PointSliceID in (624, 626, 628, 630, 632, 634, 636, 638)
        Order by UTCDateTime desc";

        using (SqlConnection connection = new SqlConnection(remoteConnectionStr))
        {
            SqlCommand command = new SqlCommand(queryStr, connection);
            command.Connection.Open();
            Dictionary<int, List<(DateTime, float)>> pointTimeValueMap = new();
            var reader = await command.ExecuteReaderAsync();
            while (reader.Read())
            {
                int point = (int)reader["PointSliceID"];
                DateTime dt = (DateTime)reader["UTCDateTime"];
                float value = (float)reader["ActualValue"];
                if (!pointTimeValueMap.ContainsKey(point))
                    pointTimeValueMap[point] = new();

                pointTimeValueMap[point].Add((dt, value));
            }
            IEnumerable<List<(DateTime, float)>> pointGeneratingEnumerator = pointTimeValueMap.Select(kvp =>
            {
                var totalList = kvp.Value;
                List<(DateTime, float)> generatingList = new();
                while (totalList.Count >= 2)
                {
                    var end = totalList[0];
                    var start = totalList[1];
                    float generatedKWHour = end.Item2 - end.Item2;
                    TimeSpan ts = start.Item1 - start.Item1;
                    float generatedKW = (float)(generatedKWHour / ts.TotalMinutes * 60);
                    generatingList.Add((start.Item1, generatedKW));
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

    async Task<Dictionary<DateTime, float>> GetConsuming()
    {
        string queryStr = @"SELECT TOP 144 
            [PointSliceID],[UTCDateTime],[ActualValue]
            FROM [JCIHistorianDB].[dbo].[tblActualValueFloat]
            Where PointSliceID = 239
            Order by UTCDateTime desc";

        using (SqlConnection connection = new SqlConnection(remoteConnectionStr))
        {
            SqlCommand command = new SqlCommand(queryStr, connection);
            command.Connection.Open();
            Dictionary<DateTime, float> consumingMap = new Dictionary<DateTime, float>();
            var reader = await command.ExecuteReaderAsync();
            while (reader.Read())
            {
                DateTime dt = (DateTime)reader["UTCDateTime"];
                float value = (float)reader["ActualValue"];
                consumingMap.Add(dt.AddHours(8), value);
            }
            return consumingMap;
        }
    }

    async Task<int> UpdateMinRecord(DateTime dt, float generating, float consuming)
    {
        string queryStr = @"INSERT INTO [dbo].[MinRecord]
           ([MonitorID]
           ,[DateTime]
           ,[generating]
           ,[storing]
           ,[consuming])
     VALUES
           (3
           ,@dt
           ,@generating
           ,null
           ,@consuming)";

        using (SqlConnection connection = new SqlConnection(localConnectionStr))
        {
            SqlCommand command = new SqlCommand(queryStr, connection);
            command.Parameters.AddWithValue("generating", generating);
            command.Parameters.AddWithValue("consuming", consuming);
            command.Parameters.AddWithValue("dt", dt);
            command.Connection.Open();
            return await command.ExecuteNonQueryAsync();
        }
    }

    private async Task ActualWork()
    {
        try
        {
            var generatingMap = await GetGenerating();
            var consumingMap = await GetConsuming();
            List<(DateTime, float, float)> updateList = new();
            foreach(var dt in generatingMap.Keys)
            {
                if (!consumingMap.ContainsKey(dt))
                    continue;

                updateList.Add((dt, generatingMap[dt], consumingMap[dt]));
            }
            updateList.Sort((a, b) => a.Item1.CompareTo(b.Item1));

        }catch(Exception ex)
        {
            Log.Error(ex, "failed");
        }
        return;
    }

    private async Task DoWorkAsync()
    {
        try
        {
            while (await _timer.WaitForNextTickAsync(_cts.Token))
            {
                Log.Information("Start doing task");
                await ActualWork();
            }
        }
        catch (OperationCanceledException)
        {

        }
    }
    public async Task StopAsync()
    {
        if (_timerTask is null) return;

        _cts.Cancel();
        await _timerTask;
        _cts.Dispose();
        Console.WriteLine("Task was canceled");
    }
}
