using Serilog;
using System.Data.SqlClient;

namespace DbMover;

internal class BackgroundTask
{
    public delegate Task BackgroundTaskGenerateDelegate();
    private readonly BackgroundTaskGenerateDelegate _taskGenerator;
    private Task? _timerTask;
    private readonly PeriodicTimer _timer;
    private readonly CancellationTokenSource _cts = new();
    public BackgroundTask(BackgroundTaskGenerateDelegate taskGenerator, TimeSpan inverval)
    {
        _taskGenerator = taskGenerator;
        _timer = new PeriodicTimer(inverval);
    }
    async public void Start()
    {
        await DoWorkAsync();
    }

    private async Task DoWorkAsync()
    {
        try
        {
            while (await _timer.WaitForNextTickAsync(_cts.Token))
            {
                Log.Information("Start doing task");
                _timerTask = _taskGenerator();
                await _timerTask;
            }
        }
        catch (OperationCanceledException)
        {

        }
    }
    public async Task StopAsync()
    {
        if (_timerTask is not null)
            await _timerTask;

        _cts.Cancel();
        _timer.Dispose();
        _cts.Dispose();

        Console.WriteLine("Task was canceled");
    }
}
