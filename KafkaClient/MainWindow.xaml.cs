using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using System.Windows;

namespace KafkaClient
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();

            Task.Factory.StartNew(() => Receive());
            Task.Factory.StartNew(()=>Send());
        }

        private void Receive()
        {
            var config = new ConsumerConfig
            {
                // two brokers on two different ports are running on localhost. BootstrapServers are used only to find the full cluster setup. So select some random brokers which are up and running.
                BootstrapServers = "localhost:9092,localhost:9192",
                GroupId = "foo",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                // make sure to subscribe before consuming.
                consumer.Subscribe(new List<string>() { "testotherdata" });
                var count = 50;
                while (count > 0)
                {
                    var res = consumer.Consume(50000);
                    Console.WriteLine(res.Value);
                    count--;
                }
            }
        }

        private async void Send()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092,localhost:9192",
                ClientId = Dns.GetHostName(),
            };


            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                var count = 5;
                while (count > 0)
                {
                    var tsk = await producer.ProduceAsync("testotherdata", new Message<Null, string>() { Value = $"message {Guid.NewGuid()}" });
                    if (tsk.Status == PersistenceStatus.Persisted)
                    {
                        Console.WriteLine("Sent!");
                    }

                    count--;
                }
            }
        }
    }
}
