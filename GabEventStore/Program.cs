using System;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace GabEventStore
{
    class Program
    {
        private static bool _connected;
        private static bool _running = true;
        private static IEventStoreConnection _connection;
        private static readonly JsonSerializerSettings SerializerSettings = new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };

        static void Main(string[] args)
        {
            var chatRoom = "toto";
            var user = "ouarzy";

            Init();
            Subscribe(
                chatRoom,
                OnChatMessageRecieved);

            while (_running)
            {
                if (_connected)
                {
                    var message = Console.ReadLine();
                    if (message == "!q")
                        _running = false;
                    else
                    {
                        SendMessage(message, user, chatRoom);
                    }
                }
                else
                {
                    Thread.Sleep(100);
                }
            }
        }

        private static void SendMessage(string text, string user, string chatRoom)
        {
            var message = new ChatMessage
            {
                User = user,
                Message = text
            };

            var json = JsonConvert.SerializeObject(message, SerializerSettings);
            var bytes = Encoding.UTF8.GetBytes(json);
            var eventData = new EventData(
                Guid.NewGuid(),
                message.GetType().ToString(),
                true,
                bytes,
                new byte[0]);

            _connection.AppendToStreamAsync(
                chatRoom,
                ExpectedVersion.Any,
                eventData);
        }

        private static void OnChatMessageRecieved(ChatMessage message)
        {
            var text = $"{message.User} says:\n{message.Message}";

            Console.WriteLine(text);
        }

        private static void Init()
        {
            var userCredentials = new UserCredentials("admin", "changeit");
            var settings = ConnectionSettings.Create()
                .UseConsoleLogger()
                .SetDefaultUserCredentials(userCredentials)
                .KeepReconnecting()
                .KeepRetrying();

            var ipAddresses = Dns.GetHostAddresses("ubuntustore.cloudapp.net");
            _connection = EventStoreConnection.Create(
                settings,
                new IPEndPoint(IPAddress.Parse(ipAddresses.First().ToString()), 1113));

            _connection.Connected += OnConnected;
            _connection.Disconnected += OnDisconnected;
            Console.WriteLine("Connecting...");
            _connection.ConnectAsync();
        }

        private static void Subscribe(
            string room,
            Action<ChatMessage> onRecieved)
        {
            _connection.SubscribeToStreamAsync(
                room,
                false,
                OnRecieved(onRecieved));

        }

        private static Action<EventStoreSubscription, ResolvedEvent> OnRecieved(Action<ChatMessage> onRecieved)
        {
            return (sender, e) =>
            {
                var json = Encoding.UTF8.GetString(e.Event.Data);
                var message = JsonConvert.DeserializeObject<ChatMessage>(json);

                onRecieved(message);
            };

        }

        private static void OnConnected(object sender, ClientConnectionEventArgs e)
        {
            Console.WriteLine("_connected");
            _connected = true;
        }

        private static void OnDisconnected(object sender, ClientConnectionEventArgs e)
        {
            Console.WriteLine("Disconnected");
            _connected = false;
        }
    }

    public class ChatMessage
    {
        public string User { get; set; }
        public string Message { get; set; }
    }

}
