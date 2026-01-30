using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQDemo.Models;

namespace RabbitMQDemo.Background
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var channel = await SetupRabbitMQ();

            bool isRunning = true;

            while (isRunning)
            {
                await channel.QueueDeclareAsync(
                    queue: "first_names",
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );

                Console.WriteLine("Awaiting Messages...");

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += async (sender, eventArgs) =>
                {
                    byte[] body = eventArgs.Body.ToArray();
                    string stringMessage = System.Text.Encoding.UTF8.GetString(body);
                    var jsonMessage = System.Text.Json.JsonSerializer.Deserialize<Message>(stringMessage);

                    Console.WriteLine($"Received: {jsonMessage.FirstName} at {jsonMessage.Timestamp:g}");

                    await channel.BasicAckAsync(eventArgs.DeliveryTag, false);
                };

                await channel.BasicConsumeAsync(
                    queue: "first_names",
                    autoAck: false,
                    consumer: consumer
                );

                await Task.Delay(7500);
            }
        }

        static async Task<IChannel> SetupRabbitMQ()
        {
            var factory = new ConnectionFactory
            {
                HostName = "rabbitmq", 
                UserName = "guest", 
                Password = "guest"
            };
            var connection = await factory.CreateConnectionAsync();
            return await connection.CreateChannelAsync();
        }
    }
}