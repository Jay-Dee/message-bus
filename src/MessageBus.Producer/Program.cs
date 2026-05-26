using MessageBus.Producer.Features.MessageGeneration;
using Microsoft.OpenApi;
using Confluent.Kafka;

public partial class Program
{
    private static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Services.AddHealthChecks();
        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSwaggerGen(c =>
        {
            c.SwaggerDoc("v1", new OpenApiInfo { Title = "MessageBus Producer", Version = "v1" });
        });

        builder.Services.AddSingleton(sp =>
        {
            var config = new ProducerConfig { BootstrapServers = "kafka:29092", };
            return new ProducerBuilder<string, string>(config).Build();
        });

        var app = builder.Build();
        if (!app.Environment.IsDevelopment())
        {
            app.UseHttpsRedirection();
        }

        app.MapHealthChecks("/");
        app.MapMessageGenerationEndpoints();
        SetupSwagger(app);

        app.Run();
    }

    private static void SetupSwagger(WebApplication app)
    {
        app.UseSwagger();
        app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "MessageBus Producer v1"));
        app.Lifetime.ApplicationStarted.Register(() =>
        {
            var serverAddresses = app.Urls; // Retrieves active listening URLs (e.g., http://localhost:5000)
            var logger = app.Services.GetRequiredService<ILogger<Program>>();

            foreach (var address in serverAddresses)
            {
                // Trims trailing slashes if present and appends the path
                string swaggerUrl = $"{address.TrimEnd('/')}/swagger/index.html";
                logger.LogInformation("Swagger UI : \u001b[36m{Url}\u001b[0m", swaggerUrl);
            }
        });
    }
}