namespace MessageBus.Consumer;

public class Worker(IMessageBusConsumer consumer, ILogger<Worker> logger) : BackgroundService
{
    private readonly IMessageBusConsumer consumer = consumer;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            await consumer.WaitForBrokerToBeAvailableAsync(stoppingToken);
            await consumer.ConsumeAsync(stoppingToken);
            await Task.Delay(5000, stoppingToken);
        }
    }
}
