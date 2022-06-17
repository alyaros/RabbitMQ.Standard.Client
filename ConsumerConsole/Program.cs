using System;
using System.Text;
using System.Text.Json;
using RabbitMQ.Standard.Client;

namespace ConsumerConsole
{
    public class WeatherForecast
    {
        public DateTime Date { get; set; }
        public int TemperatureCelsius { get; set; }
    }

    public class Program
    {
        static void Main(string[] args)
        {
            var connectionChannelPool = new ConnectionChannelPool(new ConnectionChannelPoolConfiguration("localhost"));

            var consumerConfiguration = new ConsumerConfiguration("MyExchange", "MyQueue", new List<string>() {"MyRoutingKey"});

            var consumer = new RabbitMqExchangeConsumer(consumerConfiguration, connectionChannelPool, (errorLog, exception) => Console.WriteLine($"{DateTime.UtcNow.ToString("O")} ErrorLog={errorLog} Exception={exception}"), infoLog => {Console.WriteLine($"{DateTime.UtcNow:O} Info={infoLog}");});

            consumer.OnConsumerControlMessage += delegate (object? sender, ConsumerControlMessage message)
            {
                Console.WriteLine("ControlMessage:" + JsonSerializer.Serialize(message));
            };

            consumer.OnMessageReceived += delegate (object? sender, Message message)
            {
                Console.WriteLine("RoutingKey:" + message.RoutingKey);

                if (message.Headers != null)
                    Console.WriteLine("Headers:" + JsonSerializer.Serialize(message.Headers));

                var forecastObj = JsonSerializer.Deserialize<WeatherForecast>(Encoding.ASCII.GetString(message.Body));
                Console.WriteLine("Body:" + JsonSerializer.Serialize(forecastObj));

                consumer.Acknowledge(sender);
            };

            
            consumer.StartAsync();


            Console.WriteLine("Press any key to stop consumer");
            Console.ReadKey();
            consumer.Stop();

            Console.WriteLine("Press any key to close application");
            Console.ReadKey();
        }
    }
}