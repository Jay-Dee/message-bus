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

            var batchId = Guid.NewGuid();
            var response = new MessageGenerationResponse(batchId, "ProcessingStarted");

            return Results.Accepted($"/api/messages/batch-status/{batchId}", response);
        };
    }
}

// Models remain local to the feature slice that owns them
public record MessageGenerationRequest(int NumberOfMessages, int IntervalInSeconds);
public record MessageGenerationResponse(Guid BatchId, string Status);
