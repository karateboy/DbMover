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


Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()                
                .WriteTo.Console()
                .WriteTo.File(".\\log\\DbMover.log", rollingInterval: RollingInterval.Day)
                .CreateLogger();

Console.WriteLine("Start grab exhibition db");
var task = new BackgroundTask(localBuilder.ConnectionString, remoteBuilder.ConnectionString, TimeSpan.FromHours(1));
await task.Start();
while (Console.ReadLine() != "exit");
await task.StopAsync();


