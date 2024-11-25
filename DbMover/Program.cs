// See https://aka.ms/new-console-template for more information

using DbMover;
using Serilog;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using CsvHelper;

var cliArgs = Environment.GetCommandLineArgs();
Console.WriteLine($"Args = {string.Join(",", cliArgs)}");
SqlConnectionStringBuilder localBuilder =
    new SqlConnectionStringBuilder(@"Server=localhost;Database=AQMSDB;Trusted_Connection=True;");
//setup local DB
localBuilder["Server"] = cliArgs[1];
Console.WriteLine(localBuilder.ConnectionString);

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File(".\\log\\DbMover.log", rollingInterval: RollingInterval.Day)
    .CreateLogger();

Console.WriteLine("Start grab exhibition db");
var monitorMap = new Dictionary<string, int>
{
    { "二林", 35 },
    { "朴子", 40 },
    { "崙背", 38 },
    { "臺西", 41 },
    { "麥寮", 83 },
    { "大城", 37 },
    { "斗六", 37 }
};

var mtMap = new Dictionary<string, string>()
{
    { "PM10", "04" },
    { "PM2.5", "33" },
    { "SO2", "01" },
    { "NOx", "05" },
    { "CO", "02" },
    { "O3", "03" },
    { "THC", "08" },
    { "NO", "06" },
    { "CH4", "31" },
    { "NO2", "07" },
    { "NMHC", "09" },
    { "WS_HR", "143" },
    { "WD_HR", "144" },
    { "RAINFALL", "23" },
    { "AMB_TEMP", "14" },
    { "RH", "38" }
};

List<EpaData> convert(List<DataEntry> entries)
{
    var ret = new List<EpaData>();
    void HandleEntry(DataEntry entry,int hour,  string valueStr)
    {
        try
        {
            var value = float.Parse(valueStr);
            ret.Add(new EpaData(entry.DateTime.AddHours(hour), monitorMap[entry.Monitor], 
                mtMap[entry.MonitorType], value));
        }catch(Exception e)
        {
            //Log.Information(e, "{Monitor} {MonitorType} {DateTime} {ValueStr}", 
            //    entry.Monitor, entry.MonitorType, entry.DateTime.AddHours(hour), valueStr);
        }
    }
    
    foreach (var entry in entries)
    {
        if (!monitorMap.ContainsKey(entry.Monitor))
            continue;
        
        if (!mtMap.ContainsKey(entry.MonitorType))
            continue;

        for (int i = 0; i < 24; i++)
        {
            HandleEntry(entry, i, entry.GetType().GetProperty($"hr{i}").GetValue(entry).ToString());
        }
        
    }
    Log.Information("Convert {Count} records", ret.Count);
    return ret;
}

using (var reader = new StreamReader("data.csv"))
using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
{
    var records = csv.GetRecords<DataEntry>();
    var data = convert(records.ToList());
    var dbTask = new DbTask(localBuilder.ConnectionString, data);
    int count = await dbTask.UpsertDb();
    Log.Information("Upsert {Count} records", count);
}


Log.Information("Import data complete");