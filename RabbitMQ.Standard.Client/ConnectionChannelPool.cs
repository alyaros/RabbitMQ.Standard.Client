using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using RabbitMQ.Client;

namespace RabbitMQ.Standard.Client;


public class ConnectionChannelPoolConfiguration
{
    /// <summary>
    /// Address Separated with Comma When Cluster Mode, Otherwise Address
    /// </summary>
    public string ServerAddress { get; set; }

    public string CallerName { get; set; }

    /// <summary>
    /// Default RabbitMQ
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// Default RabbitMQ
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    /// Default RabbitMQ
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Default RabbitMQ
    /// </summary>
    public int Port { get; set; } = 5672;


    public Action<ConnectionFactory> ConnectionFactoryOptions { get; set; }

    public ConnectionChannelPoolConfiguration(string serverAddress, string callerName = null)
    {
        ServerAddress = serverAddress;

        CallerName = string.IsNullOrEmpty(callerName) ? Assembly.GetEntryAssembly()?.GetName().Name.ToLower() : callerName;
    }
}

public interface IConnectionChannelPool
{
    IConnection GetConnection();

    IModel Rent();

    bool Return(IModel context);
}

public class ConnectionChannelPool : IConnectionChannelPool, IDisposable
{
    private const int DefaultPoolSize = 10;
    private readonly Func<IConnection> _connectionActivator;
    private readonly ConcurrentQueue<IModel> _pool;
    private IConnection _connection;
    private readonly object _syncLock = new object();

    private int _count;
    private int _maxSize;


    public ConnectionChannelPool(ConnectionChannelPoolConfiguration configuration)
    {
        _maxSize = DefaultPoolSize;
            
        _pool = new ConcurrentQueue<IModel>();

        _connectionActivator = CreateConnection(configuration);
    }

    public IModel Rent()
    {
        lock (_syncLock)
        {
            while (_count > _maxSize)
            {
                Thread.SpinWait(1);
            }

            if (_pool.TryDequeue(out var model))
            {
                Interlocked.Decrement(ref _count);

                Debug.Assert(_count >= 0);

                return model;
            }

            try
            {
                model = GetConnection().CreateModel();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            return model;
        }
    }

    public bool Return(IModel connection)
    {
        if (Interlocked.Increment(ref _count) <= _maxSize && connection.IsOpen)
        {
            _pool.Enqueue(connection);

            return true;
        }

        connection.Dispose();

        Interlocked.Decrement(ref _count);

        Debug.Assert(_maxSize == 0 || _pool.Count <= _maxSize);

        return false;
    }

    public IConnection GetConnection()
    {
        lock (_syncLock)
        {
            if (_connection != null && _connection.IsOpen)
            {
                return _connection;
            }

            _connection?.Dispose();
            _connection = _connectionActivator();
            return _connection;
        }
    }

    public void Dispose()
    {
        _maxSize = 0;

        while (_pool.TryDequeue(out var context))
        {
            context.Dispose();
        }
        _connection?.Dispose();
    }

    private Func<IConnection> CreateConnection(ConnectionChannelPoolConfiguration configuration)
    {
        var factory = new ConnectionFactory
        {
            UserName = configuration.UserName,
            Port = configuration.Port,
            Password = configuration.Password,
            VirtualHost = configuration.VirtualHost,
            ClientProvidedName = configuration.CallerName,
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(1)
        };

        if (configuration.ServerAddress.Contains(","))
        {
            configuration.ConnectionFactoryOptions?.Invoke(factory);

            return () => factory.CreateConnection(configuration.ServerAddress.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries));
        }

        factory.HostName = configuration.ServerAddress;
        configuration.ConnectionFactoryOptions?.Invoke(factory);
        return () => factory.CreateConnection();
    }

}