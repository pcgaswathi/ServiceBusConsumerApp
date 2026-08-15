using ServiceBusConsumerApp.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<ServiceBusReceiverService>();

builder.Services.AddHostedService(
    provider =>
        provider.GetRequiredService<ServiceBusReceiverService>());

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/api/messages", () =>
{
    var messages =
        ServiceBusReceiverService.Messages
            .Reverse()
            .ToList();

    return Results.Ok(messages);
});

app.MapGet("/api/health", () =>
{
    return Results.Ok(new
    {
        status = "Receiver is running"
    });
});

app.Run();