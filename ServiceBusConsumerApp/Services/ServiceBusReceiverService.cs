
using System.Collections.Concurrent;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using ServiceBusConsumerApp.Model;

namespace ServiceBusConsumerApp.Services;

public class ServiceBusReceiverService : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<ServiceBusReceiverService> _logger;

    private ServiceBusClient? _client;
    private ServiceBusProcessor? _processor;

    public static readonly ConcurrentQueue<UserMessage> Messages = new();

    public ServiceBusReceiverService(
        IConfiguration configuration,
        ILogger<ServiceBusReceiverService> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var connectionString =
            Environment.GetEnvironmentVariable(
                "AZURE_SERVICE_BUS_CONNECTION_STRING")
            ?? _configuration["AzureServiceBus:ConnectionString"];

        var queueName =
            Environment.GetEnvironmentVariable(
                "AZURE_SERVICE_BUS_QUEUE_NAME")
            ?? _configuration["AzureServiceBus:QueueName"]
            ?? "sender-receiver-q";

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "Azure Service Bus connection string is not configured.");
        }

        _client = new ServiceBusClient(connectionString);

        _processor = _client.CreateProcessor(
            queueName,
            new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = 1
            });

        _processor.ProcessMessageAsync += ProcessMessageAsync;

        _processor.ProcessErrorAsync += ProcessErrorAsync;

        _logger.LogInformation(
            "Starting Azure Service Bus receiver...");

        await _processor.StartProcessingAsync(stoppingToken);

        _logger.LogInformation(
            "Receiver is listening to queue: {QueueName}",
            queueName);

        try
        {
            await Task.Delay(
                Timeout.Infinite,
                stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Application is stopping
        }
    }

    private async Task ProcessMessageAsync(
        ProcessMessageEventArgs args)
    {
        try
        {
            string json =
                args.Message.Body.ToString();

            _logger.LogInformation(
                "Message received: {Message}",
                json);

            var userMessage =
                JsonSerializer.Deserialize<UserMessage>(json);

            if (userMessage != null)
            {
                userMessage.ReceivedAt =
                    DateTime.UtcNow;

                Messages.Enqueue(userMessage);

                // Keep latest 100 messages
                while (Messages.Count > 100)
                {
                    Messages.TryDequeue(out _);
                }

                _logger.LogInformation(
                    "Received message from {Name}",
                    userMessage.Name);
            }

            await args.CompleteMessageAsync(
                args.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error processing Service Bus message.");

            await args.AbandonMessageAsync(
                args.Message);
        }
    }

    private Task ProcessErrorAsync(
        ProcessErrorEventArgs args)
    {
        _logger.LogError(
            args.Exception,
            "Service Bus error. Entity: {EntityPath}",
            args.EntityPath);

        return Task.CompletedTask;
    }

    public override async Task StopAsync(
        CancellationToken cancellationToken)
    {
        if (_processor != null)
        {
            await _processor.StopProcessingAsync(
                cancellationToken);

            await _processor.DisposeAsync();
        }

        if (_client != null)
        {
            await _client.DisposeAsync();
        }

        await base.StopAsync(cancellationToken);
    }
}