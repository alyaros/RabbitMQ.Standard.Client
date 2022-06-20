using System;
using System.Collections.Generic;
using System.Linq;
using RabbitMQ.Client;

namespace RabbitMQ.Standard.Client
{
    public class SendResult
    {

        public static SendResult Success { get; } = new SendResult { Succeeded = true };

        public static SendResult Failed(Exception ex, string errorDescription = null)
        {
            var result = new SendResult
            {
                Succeeded = false,
                Exception = ex,
                ErrorDescription = errorDescription
            };

            return result;
        }


        public bool Succeeded { get; set; }
        public Exception Exception { get; set; }
        public string ErrorDescription { get; set; }

    }

    [Serializable]
    public class Message
    {
        public byte[] Body { get; }
        public IDictionary<string, string> Headers { get; }
        public string RoutingKey { get; set; }



        public Message(byte[] body, string routingKey)
        {
            Headers = null;
            Body = body;
            RoutingKey = routingKey;
        }

        public Message(IDictionary<string, string> headers, byte[] body, string routingKey)
        {
            Headers = headers;
            Body = body;
            RoutingKey = routingKey;
        }





    }

    public interface IExchangePublisher
    {
        SendResult Send(Message message);
    }

    public class PublisherConfiguration
    {
        public string ExchangeName { get; set; }

        public string ExchangeType { get; set; } = "topic";

        public PublisherConfiguration(string exchangeName)
        {
            ExchangeName = exchangeName;
        }

    }

    public class RabbitMqExchangePublisher : IExchangePublisher
    {
        private readonly PublisherConfiguration _publisherConfiguration;
        private readonly IConnectionChannelPool _connectionChannelPool;

        public RabbitMqExchangePublisher(PublisherConfiguration publisherConfiguration, IConnectionChannelPool connectionChannelPool)
        {
            _publisherConfiguration = publisherConfiguration;
            _connectionChannelPool = connectionChannelPool;
        }

        public SendResult Send(Message message)
        {
            IModel channel = null;
            try
            {
                channel = _connectionChannelPool.Rent();

                channel.ConfirmSelect();

                var basicProperties = channel.CreateBasicProperties();
                basicProperties.DeliveryMode = 2;

                if (message.Headers != null)
                    basicProperties.Headers = message.Headers.ToDictionary(x => x.Key, x => (object)x.Value);

                channel.ExchangeDeclare(_publisherConfiguration.ExchangeName, _publisherConfiguration.ExchangeType, true);

                channel.BasicPublish(_publisherConfiguration.ExchangeName, message.RoutingKey, basicProperties, message.Body);

                channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(1));

                return SendResult.Success;
            }
            catch (Exception ex)
            {
                return SendResult.Failed(ex, ex.Message);
            }
            finally
            {
                if (channel != null)
                {
                    _connectionChannelPool.Return(channel);
                }
            }
        }
    }

}




