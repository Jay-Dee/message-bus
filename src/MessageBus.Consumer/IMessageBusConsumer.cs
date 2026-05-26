namespace MessageBus.Consumer;

public interface IMessageBusConsumer
{
    Task<bool> WaitForBrokerToBeAvailableAsync(CancellationToken cancellationToken);
    Task ConsumeAsync(CancellationToken cancellationToken);
}   