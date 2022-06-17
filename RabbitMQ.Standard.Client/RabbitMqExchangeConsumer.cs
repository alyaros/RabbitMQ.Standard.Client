using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Standard.Client
{

    public class ConsumerConfiguration
    {

        public string ExchangeName { get; set; }
        public string QueueName { get; }

        public IEnumerable<string> RoutingKeys { get; set; }


        public string ExchangeType { get; set; } = "topic";
        public int MessageTimeToLiveInMilliseconds { get; set; } = 864000000;


        /// <summary>
        /// If Queue was already created in server with this definition Set to X,
        /// You'll need to delete the queue and only afterwards run it with newer value Y.
        /// </summary>
        public int QueueMaxNumberOfMessages { get; set; } = 250000;


        public ConsumerConfiguration(string exchangeName, string queueName, IEnumerable<string> routingKeys)
        {
            ExchangeName = exchangeName;
            QueueName = queueName;

            RoutingKeys = routingKeys ?? throw new ArgumentNullException(nameof(routingKeys));
        }
    }

    public class RabbitMqExchangeConsumer : IExchangeConsumer
    {

        public event EventHandler<Message> OnMessageReceived;

        public event EventHandler<ConsumerControlMessage> OnConsumerControlMessage;


        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(initialCount: 1, maxCount: 1);

        private readonly IConnectionChannelPool _connectionChannelPool;
        private readonly ConsumerConfiguration _configuration;
        private IModel _channel;
        private IConnection _connection;


        private readonly Action<string, Exception> _onErrorLog;
        private readonly Action<string> _onInfoLog;

        public RabbitMqExchangeConsumer(ConsumerConfiguration configuration, ConnectionChannelPool connectionChannelPool, Action<string, Exception> onErrorLog, Action<string> onInfoLog)
        {
            _configuration = configuration;
            _connectionChannelPool = connectionChannelPool;
            _onErrorLog = onErrorLog;
            _onInfoLog = onInfoLog;
        }





        public void StartAsync()
        {
            // Run LongRunning tasks on their own dedicated thread.

            Task.Factory.StartNew(() =>
            {
                try
                {
                    _onInfoLog?.Invoke($"Starting Consumer Thread");

                    Start(TimeSpan.FromSeconds(2), _cancellationTokenSource.Token);
                }
                catch (OperationCanceledException)
                {
                    //Ignore
                }
                catch (Exception e)
                {
                    _onErrorLog?.Invoke($"{nameof(StartAsync)} Failed", e);
                }
            }, _cancellationTokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public void Stop()
        {
            _onInfoLog?.Invoke($"Stopping Consumer Thread");

            _cancellationTokenSource.Cancel();
        }

        public void Acknowledge(object sender)
        {
            if (_channel!.IsOpen)
            {
                _channel.BasicAck((ulong)sender, false);
            }
        }

        public void Reject(object sender)
        {
            if (_channel!.IsOpen && sender is ulong val)
            {
                _channel.BasicReject(val, true);
            }
        }

        public void Dispose()
        {
            _cancellationTokenSource.Dispose();

            _channel?.Dispose();

            //The connection should not be closed here, because the connection is still in use elsewhere. 
            //_connection?.Dispose();
        }






        private void Start(TimeSpan timeout, CancellationToken cancellationToken)
        {
            Connect();

            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += OnConsumerReceived;
            consumer.Shutdown += OnConsumerShutdown;
            consumer.Registered += OnConsumerRegistered;
            consumer.Unregistered += OnConsumerUnregistered;
            consumer.ConsumerCancelled += OnConsumerCancelled;

            _channel.BasicConsume(_configuration.QueueName, false, consumer);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                cancellationToken.WaitHandle.WaitOne(timeout);
            }

            // ReSharper disable once FunctionNeverReturns
        }

        private void Connect()
        {
            if (_connection != null)
            {
                return;
            }

            _connectionLock.Wait();

            try
            {
                if (_connection == null)
                {
                    _connection = _connectionChannelPool.GetConnection();

                    _channel = _connection.CreateModel();

                    _channel.ExchangeDeclare(_configuration.ExchangeName, _configuration.ExchangeType, true);

                    var arguments = new Dictionary<string, object>
                    {
                        {"x-message-ttl", _configuration.MessageTimeToLiveInMilliseconds},
                        {"x-max-length", _configuration.QueueMaxNumberOfMessages},
                        {"x-overflow", "reject-publish"}
                    };

                    _channel.QueueDeclare(_configuration.QueueName, durable: true, exclusive: false, autoDelete: false, arguments: arguments);

                    foreach (var routingKey in _configuration.RoutingKeys)
                        _channel.QueueBind(_configuration.QueueName, _configuration.ExchangeName, routingKey);

                    _onInfoLog?.Invoke("Queue Binding Created");
                }
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        private void OnConsumerReceived(object sender, BasicDeliverEventArgs e)
        {
            try
            {
                var headers = new Dictionary<string, string>();

                if (e.BasicProperties.Headers != null)
                {
                    foreach (var header in e.BasicProperties.Headers)
                    {
                        if (header.Value is byte[] val)
                        {
                            headers.Add(header.Key, Encoding.UTF8.GetString(val));
                        }
                        else
                        {
                            headers.Add(header.Key, header.Value?.ToString());
                        }
                    }
                }

                var message = new Message(headers, e.Body.ToArray(), e.RoutingKey);

                OnMessageReceived?.Invoke(e.DeliveryTag, message);
            }
            catch (Exception ex)
            {
                _onErrorLog?.Invoke($"{nameof(OnConsumerReceived)} Failed",ex);
            }
        }

        private void OnConsumerCancelled(object sender, ConsumerEventArgs e)
        {
            try
            {
                var args = new ConsumerControlMessage
                {
                    MessageType = ConsumerControlMessageType.ConsumerCancelled,
                    Reason = string.Join(",", e.ConsumerTags)
                };

                _onInfoLog?.Invoke($"On Consumer Cancelled ConsumerTags={args.Reason}");

                OnConsumerControlMessage?.Invoke(sender, args);
            }
            catch (Exception ex)
            {
                _onErrorLog?.Invoke($"{nameof(OnConsumerCancelled)} Failed", ex);
            }
        }

        private void OnConsumerUnregistered(object sender, ConsumerEventArgs e)
        {
            try
            {
                var args = new ConsumerControlMessage
                {
                    MessageType = ConsumerControlMessageType.ConsumerUnregistered,
                    Reason = string.Join(",", e.ConsumerTags)
                };

                _onInfoLog?.Invoke($"On Consumer Unregistered ConsumerTags={args.Reason}");

                OnConsumerControlMessage?.Invoke(sender, args);
            }
            catch (Exception ex)
            {
                _onErrorLog?.Invoke($"{nameof(OnConsumerUnregistered)} Failed", ex);
            }
        }

        private void OnConsumerRegistered(object sender, ConsumerEventArgs e)
        {
            try
            {
                var args = new ConsumerControlMessage
                {
                    MessageType = ConsumerControlMessageType.ConsumerRegistered,
                    Reason = string.Join(",", e.ConsumerTags)
                };

                OnConsumerControlMessage?.Invoke(sender, args);
            }
            catch (Exception ex)
            {
                _onErrorLog?.Invoke($"{nameof(OnConsumerRegistered)} Failed", ex);
            }
        }

        private void OnConsumerShutdown(object sender, ShutdownEventArgs e)
        {
            try
            {
                var args = new ConsumerControlMessage
                {
                    MessageType = ConsumerControlMessageType.ConsumerShutdown,
                    Reason = e.ReplyText
                };

                _onInfoLog?.Invoke($"On Consumer Shutdown ReplyText={args.Reason}");

                OnConsumerControlMessage?.Invoke(sender, args);
            }
            catch (Exception ex)
            {
                _onErrorLog?.Invoke($"{nameof(OnConsumerShutdown)} Failed", ex);
            }
        }


    }

    public interface IExchangeConsumer : IDisposable
    {
        event EventHandler<Message> OnMessageReceived;

        event EventHandler<ConsumerControlMessage> OnConsumerControlMessage;

        void StartAsync();
        void Stop();
        void Acknowledge(object sender);
        void Reject(object sender);
    }

    public enum ConsumerControlMessageType
    {
        ConsumerCancelled,
        ConsumerRegistered,
        ConsumerUnregistered,
        ConsumerShutdown
    }

    public class ConsumerControlMessage : EventArgs
    {
        public string Reason { get; set; }

        public ConsumerControlMessageType MessageType { get; set; }
    }


}
