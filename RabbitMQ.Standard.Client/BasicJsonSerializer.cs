using System.Text.Json;

namespace RabbitMQ.Standard.Client;

public class BasicJsonSerializer
{
    private readonly JsonSerializerOptions _serializerOptions;

    public BasicJsonSerializer()
    {
        _serializerOptions = new();
    }

    public byte[] Serialize(object rawObject)
    {
        return JsonSerializer.SerializeToUtf8Bytes(rawObject, options: _serializerOptions);
    }

    public T Deserialize<T>(byte[] serializedObject)
    {
        return JsonSerializer.Deserialize<T>(serializedObject, options: _serializerOptions)!;
    }
}