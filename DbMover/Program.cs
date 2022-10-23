// See https://aka.ms/new-console-template for more information
using DbMover;
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
int backPort = 0;
if(cliArgs.Length > 5)
    backPort = int.Parse(cliArgs[5]);

Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()                
                .WriteTo.Console()
                .WriteTo.File(".\\log\\DbMover.log", rollingInterval: RollingInterval.Day)
                .CreateLogger();

Console.WriteLine("Start grab exhibition db");
var dbTask = new DbTask(localBuilder.ConnectionString, remoteBuilder.ConnectionString);

if(backPort > 0)
{
    List<Task> taskList = new();
    for (int i = -backPort; i <= -1; i++)
    {
        taskList.Add(dbTask.GrabRemoteDbAt(DateTime.Now.AddDays(i)));
    }
    await Task.WhenAll(taskList);
}

await dbTask.GrabRemoteDbAt(DateTime.Now.AddDays(-1));
Log.Information("Move remote DB complete");
