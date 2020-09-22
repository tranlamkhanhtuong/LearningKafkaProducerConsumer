using Confluent.Kafka;
using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Producer");
            var config = new ProducerConfig
            {
                BootstrapServers = ConfigurationManager.AppSettings["BootstrapServers"].Trim().ToString()
            };
            var topic = ConfigurationManager.AppSettings["topic"].Trim().ToString();

            Console.Write("Total message: ");
            var totalMsg = int.Parse(Console.ReadLine());
         

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                #region old
                //try
                //{
                //    var dr = await p.ProduceAsync("first_topic", new Message<string, string> { Key = "tuong", Value = Newtonsoft.Json.JsonConvert.SerializeObject(new { foo = "bar 2" }) });
                //    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                //}
                //catch (ProduceException<Null, string> e)
                //{
                //    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                //}
                #endregion

                int numProduced = 0;
                //int numMessages = 10;
                for (int i = 0; i < totalMsg; ++i)
                {
                    //var key = "key"+i.ToString();
                    var val = Newtonsoft.Json.JsonConvert.SerializeObject(new { foo = "tuong: " + i.ToString() });

                    Console.WriteLine($"Producing record: {val}");

                    producer.Produce(topic, new Message<Null, string> {Value = val },
                        (deliveryReport) =>
                        {
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                            }
                            else
                            {
                                Console.WriteLine($"Produced message to: {deliveryReport.TopicPartitionOffset}");
                                numProduced += 1;
                            }
                        });
                    //Thread.Sleep(1000);
                }
                // wait for up to 10 seconds for any inflight messages to be delivered.
                producer.Flush(TimeSpan.FromSeconds(10));
                Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
            }

            Console.ReadKey();
        }
    }
}
