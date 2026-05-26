using Confluent.Kafka;
using System.Text.Json;

namespace MessageBus.Producer.Features.MessageGeneration;

public static class MessageGenerationEndpoints
{
    public static IEndpointRouteBuilder MapMessageGenerationEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/messages")
            .WithTags("Message Generation");

        group.MapPost("/generate", GenerateMessages())
        .WithName("GenerateMessages")
        .WithSummary("Asynchronously streams a controlled batch of events into the message bus.")
        .Produces<MessageGenerationResponse>(StatusCodes.Status202Accepted)
        .Produces(StatusCodes.Status400BadRequest);

        return app;
    }

    private static Func<MessageGenerationRequest, ILoggerFactory, Task<IResult>> GenerateMessages()
    {
        return async (
                    MessageGenerationRequest request,
                    ILoggerFactory loggerFactory) =>
        {
            var logger = loggerFactory.CreateLogger("MessageGeneration");
            logger.LogInformation("Received request to generate {NumberOfMessages} messages with an interval of {IntervalInSeconds} seconds.", request.NumberOfMessages, request.IntervalInSeconds);
            if(request.NumberOfMessages <= 0 || request.IntervalInSeconds < 0)
            {
                logger.LogWarning("Invalid request parameters: NumberOfMessages={NumberOfMessages}, IntervalInSeconds={IntervalInSeconds}", request.NumberOfMessages, request.IntervalInSeconds);
                return Results.BadRequest("NumberOfMessages must be greater than 0 and IntervalInSeconds must be non-negative.");
            }
            else
            {
                // Simulate asynchronous message generation and sending to the message bus
                _ = Task.Run(async () =>
                {
                    for (int i = 0; i < request.NumberOfMessages; i++)
                    {
                        var message = new
                        {
                            Id = Guid.NewGuid(),
                            Timestamp = DateTime.UtcNow,
                            SequenceNumber = i + 1
                        };
                        var messageJson = JsonSerializer.Serialize(message);
                        logger.LogInformation("Generated message {SequenceNumber}: {MessageJson}", message.SequenceNumber, messageJson);
                        // Simulate sending to message bus (e.g., Kafka)
                        await Task.Delay(request.IntervalInSeconds * 1000);
                    }
                    logger.LogInformation("Completed generating {NumberOfMessages} messages.", request.NumberOfMessages);
                });
            }
            var batchId = Guid.NewGuid();
            var response = new MessageGenerationResponse(batchId, "ProcessingStarted");

            return Results.Accepted($"/api/messages/batch-status/{batchId}", response);
        };
    }
}

// Models remain local to the feature slice that owns them
public record MessageGenerationRequest(int NumberOfMessages, int IntervalInSeconds);
public record MessageGenerationResponse(Guid BatchId, string Status);
