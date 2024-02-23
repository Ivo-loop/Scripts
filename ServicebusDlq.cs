using Azure.Messaging.ServiceBus;

await Run();
return;

async Task Run()
{
    const string connectionString = "connection-string";
    const string topicName = "topic";
    const string subscriberName = "subscription";
    var client = new ServiceBusClient(connectionString);
    var sender = client.CreateSender(topicName);

    var dlqReceiver = client.CreateReceiver(topicName, subscriberName,
        new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

    for (var i = 0; i < 10000; i++)
    {
        await ProcessTopicAsync(sender, topicName, subscriberName, dlqReceiver);
    }

    await dlqReceiver.CloseAsync();
    await sender.CloseAsync();
    await client.DisposeAsync();
}

static async Task ProcessTopicAsync(ServiceBusSender sender, string topicName, string subscriberName, ServiceBusReceiver receiver, int fetchCount = 10)
{
    try
    {
        await ProcessDeadLetterMessagesAsync($"topic: {topicName} -> subscriber: {subscriberName}", fetchCount,
            sender, receiver);
    }
    catch (ServiceBusException ex)
    {
        if (ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
        {
            Console.WriteLine($"Topic:Subscriber '{topicName}:{subscriberName}' not found. Check that the name provided is correct.");
        }
        else
        {
            throw;
        }
    }
}

static async Task ProcessDeadLetterMessagesAsync(
    string source, int fetchCount, ServiceBusSender sender, ServiceBusReceiver dlqReceiver)
{
    var wait = new TimeSpan(0, 0, 10);

    Console.WriteLine($"fetching messages ({wait.TotalSeconds} seconds retrieval timeout)");
    Console.WriteLine(source);

    IReadOnlyList<ServiceBusReceivedMessage> dlqMessages = await dlqReceiver.ReceiveMessagesAsync(fetchCount, wait);

    Console.WriteLine($"dl-count: {dlqMessages.Count}");

    var i = 1;

    foreach (var dlqMessage in dlqMessages)
    {
        Console.WriteLine($"start processing message {i}");
        Console.WriteLine($"dl-message-dead-letter-message-id: {dlqMessage.MessageId}");
        Console.WriteLine($"dl-message-dead-letter-reason: {dlqMessage.DeadLetterReason}");
        Console.WriteLine($"dl-message-dead-letter-error-description: {dlqMessage.DeadLetterErrorDescription}");

        var resubmittableMessage = new ServiceBusMessage(dlqMessage)
        {
            MessageId = Guid.NewGuid().ToString()
        };

        await sender.SendMessageAsync(resubmittableMessage);

        await dlqReceiver.CompleteMessageAsync(dlqMessage);

        Console.WriteLine($"finished processing message {i}");
        Console.WriteLine("--------------------------------------------------------------------------------------");

        i++;
    }

    Console.WriteLine("finished");
}
