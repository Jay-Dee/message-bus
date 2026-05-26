namespace MessageBus.Consumer;

public class Worker(IMessageBusConsumer consumer, ILogger<Worker> logger) : BackgroundService
{
    private readonly IMessageBusConsumer consumer = consumer;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            var brokerIsAvailable = await consumer.WaitForBrokerToBeAvailableAsync(stoppingToken);
            if (brokerIsAvailable)
            {
                logger.LogInformation("Kafka broker is available. Starting to consume messages...");
                while(!stoppingToken.IsCancellationRequested)
                {
                    await consumer.ConsumeAsync(stoppingToken);
                }
            }
            else
            {
                logger.LogWarning("Kafka broker is not available. Retrying in 5 seconds...");
                await Task.Delay(5000, stoppingToken);
            }
        }
    }
}
