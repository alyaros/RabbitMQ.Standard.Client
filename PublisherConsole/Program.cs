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
            var connectionChannelPool = new ConnectionChannelPool(new ConnectionChannelPoolConfiguration("localhost"));

            var publisher = new RabbitMqExchangePublisher(new PublisherConfiguration("MyExchange"), connectionChannelPool);

            for (int i = 0; i < 10; i++)
            {
                string jsonString = JsonSerializer.Serialize(new WeatherForecast() { Date = DateTime.UtcNow, TemperatureCelsius = i });
                byte[] bytes = Encoding.ASCII.GetBytes(jsonString);

                publisher.Send(new Message(bytes, "MyRoutingKey"));
            }


            Console.WriteLine("Publisher Done");
            Console.ReadKey();

        }
    }
}