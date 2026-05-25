namespace MessageBus.Consumer;

public interface IMessageBusConsumer
{
    Task ConsumeAsync(CancellationToken cancellationToken);
}   