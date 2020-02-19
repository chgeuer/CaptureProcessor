namespace ConsoleAppTest
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.CaptureProcessor;
    using Microsoft.Azure.EventHubs.Processor;
    using Newtonsoft.Json;

    internal class Program
    {
        private static async Task Main()
        {
            var credentialData = JsonConvert.DeserializeObject<dynamic>(
                await File.ReadAllTextAsync(
                    Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "creds.json")));
            var projectConfig = credentialData.flightbooking;

            var captureProcessorHost = new CaptureProcessorHost(
                eventHubConnectionString: (string)projectConfig.sharedAccessConnectionStringRoot, // "Endpoint=sb://...servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=..."
                namespaceName: (string)projectConfig.entityPath,
                eventHubName: (string)projectConfig.eventHubName,
                partitionCount: 1,
                consumerGroup: "$Capture",
                leaseContainerName: "leases",
                captureStorageAccountConnectionString: (string)projectConfig.capture.storageConnectionString, // "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
                captureContainerName: (string)projectConfig.capture.containerName,
                captureFileNameFormat: "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}",
                startingAt: DateTime.Now.Subtract(TimeSpan.FromDays(6)));

            await captureProcessorHost.RunCaptureProcessorAsync(() => new ConsoleWriterProcessor());
        }
    }

    public class ConsoleWriterProcessor : IEventProcessor
    {
        public async Task OpenAsync(PartitionContext context)
        {
            await Console.Out.WriteLineAsync($"Open Partition {context.PartitionId}");
        }

        private static string FormatMessage(EventData m) => string.Join(" ", new[]
            {
                "     ",
                $"enqueuedTimeUtc={m.SystemProperties.EnqueuedTimeUtc.ToString("yyyy/MM/dd/HH/mm/ss")}",
                $"sequenceNumber={m.SystemProperties.SequenceNumber}",
                $"offset=\"{m.SystemProperties.Offset}\"",
                $"body=\"{m.Body.ToArray().ToUtf8String()}\""
            });

        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> m) =>
                m.ForeachAwaiting(msg => Console.Out.WriteLineAsync(FormatMessage(msg)));

        public Task ProcessErrorAsync(PartitionContext context, Exception error) { return Task.CompletedTask; }
        public Task CloseAsync(PartitionContext context, CloseReason reason) { return Task.CompletedTask; }
    }

    public static class MyExtensions
    {
        public static string ToUtf8String(this byte[] bytes) => Encoding.UTF8.GetString(bytes);
        public static async Task ForeachAwaiting<T>(this IEnumerable<T> values, Func<T, Task> action)
        {
            foreach (var value in values)
            {
                await action(value);
            }
        }
    }
}