using System;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using Newtonsoft.Json;
using Newtonsoft.Json.Schema;
using Newtonsoft.Json.Serialization;

namespace GabEventStoreMob
{
    class Program
    {
        private static readonly JsonSerializerSettings SerializerSettings = 
            new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };

        private static IEventStoreConnection conn;
        private static bool _running;
        private static bool _connected;

        static void Main(string[] args)

        {
            Console.WriteLine("Début du code");
                       var ipAddresses = Dns.GetHostAddresses("ubuntustore.cloudapp.net");
            var connectionSettings = ConnectionSettings.Create().UseConsoleLogger()
                .SetDefaultUserCredentials(new UserCredentials("admin","changeit"));


            InitConnection(connectionSettings, ipAddresses);

            Subscribe();

            StreamEventsSlice evts = conn.ReadStreamEventsForwardAsync("Room0", 0, 100, true).Result;
            foreach (var resolvedEvent in evts.Events)
            {
                OnEventRecieved(null, resolvedEvent);
            }

            _running = true;

            while (_running)
            {
                if (_connected)
                {
                    var message = Console.ReadLine();
                    if (message == "!q")
                        _running = false;
                    else
                    {
                        SendMessage(message);
                    }
                }
                else
                {
                    Thread.Sleep(100);
                }
            }
            
        }

        private static void SendMessage(String mess)
        {
            conn.AppendToStreamAsync("RoomXMO", ExpectedVersion.Any, CreateEventData(mess));
        }

        private static void Subscribe()
        {
            conn.SubscribeToStreamAsync("Room0", true, OnEventRecieved);
            conn.SubscribeToStreamAsync("commetuveux", true, OnEventRecieved);
            conn.SubscribeToStreamAsync("RoomXMO", true, OnEventRecieved);
        }

        private static void InitConnection(ConnectionSettingsBuilder connectionSettings, IPAddress[] ipAddresses)
        {
            conn = EventStoreConnection.Create(
                connectionSettings,
                new IPEndPoint(IPAddress.Parse(ipAddresses.First().ToString()), 1113));

            conn.Connected += ConnOnConnected;
            conn.Closed += ConnOnClosed;
            conn.ErrorOccurred += ConnOnErrorOccurred;

            conn.ConnectAsync();
        }

        private static void OnEventRecieved(EventStoreSubscription es, ResolvedEvent e)
        {
            var json = Encoding.UTF8.GetString(e.Event.Data);
            var message = JsonConvert.DeserializeObject<ChatMessage>(json);

            if (message == null)
            {
                Console.WriteLine(e.Event.EventStreamId + "/Undefined");
            }
            else
            {
                
                Console.WriteLine(e.Event.EventStreamId + "/" + message.User + ":" + message.Message);
            }
            
        }

        private static EventData[] CreateEventData(String mess)
        {
            return new EventData[]
            {
                new EventData(Guid.NewGuid(), "ChatMessage", true, CreateMessage(mess), new byte[0])
            };
        }

        private static byte[] CreateMessage(String mess)
        {
            var message = new ChatMessage
            {
                User = "Xavier",
                Message = mess
            };

            var json = JsonConvert.SerializeObject(message, SerializerSettings);
            var bytes = Encoding.UTF8.GetBytes(json);

            return bytes;

        }

        private static void ConnOnErrorOccurred(object sender, ClientErrorEventArgs clientErrorEventArgs)
        {
            Console.WriteLine("Erreur");
        }

        private static void ConnOnClosed(object sender, ClientClosedEventArgs clientClosedEventArgs)
        {
            Console.WriteLine("Disconnected");
        }


        private static void ConnOnConnected(object sender, ClientConnectionEventArgs clientConnectionEventArgs)
        {
            _connected = true;
            Console.WriteLine("connected");
        }

        public class ChatMessage
        {
            public string User { get; set; }
            public string Message { get; set; }
        }
    }
}
