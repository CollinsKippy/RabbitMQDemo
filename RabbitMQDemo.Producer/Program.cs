using System.Net.Sockets;
using System.Text;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RabbitMQDemo.Models;

namespace RabbitMQDemo.Producer
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var channel = await SetupRabbitMQ();

            await channel.QueueDeclareAsync(
                queue: "first_names",
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);


            bool isRunning = true;

            while (isRunning)
            {
                string name = GetUserInput();

                if (name == "q")
                {
                    isRunning = false;
                    break;
                }

                Console.WriteLine($"Serializing and Publishing Name: {name}");

                string jsonLog = System.Text.Json.JsonSerializer.Serialize(new Message
                {
                    Timestamp = DateTime.Now,
                    FirstName = name,
                });

                byte[] body = System.Text.Encoding.UTF8.GetBytes(jsonLog);

                await channel.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: "first_names",
                    basicProperties: new BasicProperties { Persistent = true },
                    body: body,
                    mandatory: true);

                await Task.Delay(5000);
            }
        }

        private static async Task<IChannel> SetupRabbitMQ()
        {
            var factory = new ConnectionFactory
            {
                HostName = "rabbitmq",
                Port = 5672,
                UserName = "guest",
                Password = "guest"
            };

            var retryPolicy = Policy
                .Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .Or<TimeoutException>()
                .WaitAndRetryAsync(
                    retryCount: 5,
                    sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                    onRetry: (exception, timeSpan, retry, context) =>
                    {
                        Console.WriteLine($"Retry attempt {retry} after {timeSpan.TotalSeconds}s: {exception.Message}");
                    });

            IConnection connection = await retryPolicy.ExecuteAsync(() => factory.CreateConnectionAsync());
            return await connection.CreateChannelAsync();

        }

        /**
         * Get User First Name
         */
        static string GetUserInput()
        {
            while (true)
            {
                Console.WriteLine("----");
                Console.Write("Enter your first name (or q to exit): ");
                Console.WriteLine("----");
                var userInput = Console.ReadLine();
                if (string.IsNullOrEmpty(userInput))
                {
                    Console.WriteLine("Incorrect input, please try again");
                }
                else
                {
                    return userInput;
                }
            }
        }
    }
}