using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

Console.WriteLine("DotNet Pulsar Producer");

// https://pulsar.apache.org/docs/en/client-libraries-dotnet/
// https://github.com/apache/pulsar-dotpulsar/tree/master/samples/Producing
// https://github.com/apache/pulsar-dotpulsar/blob/master/samples/Consuming
// build executable dotnet publish --configuration Release
// Executable is produced here:
// bin/Release/net6.0/publish/example
var cts = new CancellationTokenSource();

Console.CancelKeyPress += (sender, args) =>
{
    cts.Cancel();
    args.Cancel = true;
};

//var myTopic = "persistent://public/default/dotnettest";
var meetupTopic = "persistent://meetup/newjersey/first";
var systemUri = new System.Uri("pulsar://pulsar1:6650");
var client = PulsarClient.Builder()
                .ServiceUrl(systemUri)
                .Build();

var producer = client.NewProducer()
                     .Topic(meetupTopic)
                     .Create();

var dateNow = DateTime.UtcNow.ToLongTimeString();
var data = Encoding.UTF8.GetBytes("{\"ts\":\""+ dateNow + "\"}");
var messageId = await producer.NewMessage()
                              .Property("key", dateNow)
                              .Send(data);

string utfString = Encoding.UTF8.GetString(data, 0, data.Length);                                 
Console.WriteLine("Sent: " + utfString);

await using var consumer = client.NewConsumer(Schema.String)
            .SubscriptionName("dotnet-consumer")
            .Topic(meetupTopic)
            .Create();

try
{
    await foreach (var message in consumer.Messages(cts.Token))
    {
        Console.WriteLine("Received: " + message.Value());
        await consumer.Acknowledge(message, cts.Token);
    }
}
catch (OperationCanceledException) { }
