namespace MessageBus.Consumer;

using System.Threading;
using Confluent.Kafka;

public class HardCodedMessageBusConsumer : IMessageBusConsumer
{
    private readonly IConsumer<string, string> consumer;
    private readonly ILogger<HardCodedMessageBusConsumer> logger;

    public HardCodedMessageBusConsumer(ILogger<HardCodedMessageBusConsumer> logger)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "kafka:29092",
            GroupId = "messagebus-demo-consumer-group#1", 
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe("messagebus-demo-topic"); 
        this.logger = logger;
    }

    public Task ConsumeAsync(CancellationToken cancellationToken)
    {
        try
        {
            logger.LogInformation("Consuming message from hard coded consumer...");
            var consumeResult = consumer.Consume(cancellationToken);
            if (consumeResult != null)
            {
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