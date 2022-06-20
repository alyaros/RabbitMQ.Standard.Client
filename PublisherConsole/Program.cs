using System;
using System.Text;
using System.Text.Json;
using RabbitMQ.Standard.Client;

namespace PublisherConsole
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
            var connectionChannelPool = new ConnectionChannelPool(new ConnectionChannelPoolConfiguration("127.0.0.1"));

            var publisher = new RabbitMqExchangePublisher(new PublisherConfiguration("MyExchange"), connectionChannelPool);

            for (int i = 0; i < 100000000; i++)
            {
                string jsonString = JsonSerializer.Serialize(new WeatherForecast() { Date = DateTime.UtcNow, TemperatureCelsius = i });
                byte[] bytes = Encoding.ASCII.GetBytes(jsonString);

                var r = publisher.Send(new Message(bytes, "MyRoutingKey"));
                Console.WriteLine(!r.Succeeded ? r.ErrorDescription : "Published");

                Thread.Sleep(1000);
            }


            Console.WriteLine("Publisher Done");
            Console.ReadKey();

        }
    }
}