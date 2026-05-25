using MessageBus.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<IMessageBusConsumer, HardCodedMessageBusConsumer>();
builder.Services.AddHostedService<Worker>();

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .CreateLogger();

builder.Logging.ClearProviders();
builder.Services.AddSerilog();

var host = builder.Build();
host.Run();
