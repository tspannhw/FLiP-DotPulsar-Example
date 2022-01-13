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

var systemUri = new System.Uri("pulsar://pulsar1:6650");
var client = PulsarClient.Builder()
                .ServiceUrl(systemUri)
                .Build();

var producer = client.NewProducer()
                     .Topic("persistent://public/default/dotnettest")
                     .Create();

var dateNow = DateTime.UtcNow.ToLongTimeString();
var data = Encoding.UTF8.GetBytes("{\"ts\":\""+ dateNow + "\"}");
var messageId = await producer.NewMessage()
                              .Property("key", dateNow)
                              .Send(data);

string utfString = Encoding.UTF8.GetString(data, 0, data.Length);                                 
Console.WriteLine("Sent: " + utfString);