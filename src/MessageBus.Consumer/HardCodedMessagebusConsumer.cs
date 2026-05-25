namespace MessageBus.Consumer;

using System.Threading;
using Confluent.Kafka;

public class HardCodedMessageBusConsumer : IMessageBusConsumer
{
    private readonly ConsumerConfig config;
    private IConsumer<string, string> consumer;
    private readonly ILogger<HardCodedMessageBusConsumer> logger;

    public HardCodedMessageBusConsumer(ILogger<HardCodedMessageBusConsumer> logger)
    {
        config = new ConsumerConfig
        {
            BootstrapServers = "kafka:29092",
            GroupId = "messagebus-demo-consumer-group#1", 
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

       
        this.logger = logger;
    }

    public async Task<bool> WaitForBrokerToBeAvailableAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = config.BootstrapServers }).Build();
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(3));

                if (metadata != null && metadata.Brokers.Count > 0)
                {
                    logger.LogInformation("Kafka server discovered successfully with {Count} brokers.", metadata.Brokers.Count);
                    consumer = new ConsumerBuilder<string, string>(config).Build();
                    consumer.Subscribe("messagebus-demo-topic");
                    return true; // Connection test passed
                }
                logger.LogInformation("Successfully connected to Kafka broker.");
                return true;
            }
            catch (KafkaException ex)
            {
                logger.LogWarning("Failed to connect to Kafka broker. Retrying in 5 seconds...");
                
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
            catch(Exception ex)
            {
                logger.LogError(ex, "An unexpected error occurred while trying to connect to Kafka broker.");
                throw;
            }
        }

        return false; // Operation was cancelled
    }

    public Task ConsumeAsync(CancellationToken cancellationToken)
    {
        try
        {
            logger.LogInformation("Consuming message from hard coded consumer...");
            var consumeResult = consumer.Consume(cancellationToken);
            if (consumeResult != null)
            {
                consumer.Commit(consumeResult);
                logger.LogInformation("Consumed message: {message} at: {time}", consumeResult.Message.Value, DateTimeOffset.Now);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Message consumption cancelled. Most likely a timeout.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occurred while consuming messages.");
        }
        return Task.CompletedTask;
    }
}