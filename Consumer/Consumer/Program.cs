using Confluent.Kafka;
using System;
using System.Configuration;
using System.Threading;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Consumer");
            var conf = new ConsumerConfig
            {
                GroupId = ConfigurationManager.AppSettings["GroupId"].Trim().ToString(),
                BootstrapServers = ConfigurationManager.AppSettings["BootstrapServers"].Trim().ToString(),
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<string, string>(conf).Build())
            {
                c.Subscribe(ConfigurationManager.AppSettings["topic"].Trim().ToString());

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {

                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Partition {cr.Partition} - Key {cr.Key} - Offset '{cr.Offset}' Timestamp {cr.Message.Timestamp.UnixTimestampMs} - Consumed message '{cr.Value}' - Topic {cr.Topic} - at: '{cr.TopicPartitionOffset}' TopicPartition {cr.TopicPartition}");
                            Thread.Sleep(1000);
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
    }
}
