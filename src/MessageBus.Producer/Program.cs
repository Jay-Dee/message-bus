using Microsoft.OpenApi;

var builder = WebApplication.CreateBuilder(args);

// 1. BEST PRACTICE: Register Native Health Check Services
builder.Services.AddHealthChecks();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo { Title = "MessageBus Producer", Version = "v1" });
});

var app = builder.Build();
if (!app.Environment.IsDevelopment())
{
    app.UseHttpsRedirection();
}


// 2. BEST PRACTICE: Map Health Check Endpoint to the Root "/"
// This returns HTTP 200 "Healthy" when the app is alive, and works cleanly with orchestrators.
app.MapHealthChecks("/");

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

app.Run();
