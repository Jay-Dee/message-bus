namespace MessageBus.Consumer;

public class Worker(IMessageBusConsumer consumer, ILogger<Worker> logger) : BackgroundService
{
    private readonly IMessageBusConsumer consumer = consumer;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(5));
        while (!stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            await consumer.ConsumeAsync(timeoutCts.Token);
            await Task.Delay(5000, stoppingToken);
        }
    }
}
