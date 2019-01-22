open System
open System.Net
open EventStore.ClientAPI
open Newtonsoft.Json
open EventStore.ClientAPI.SystemData
open System.Threading


type ChatMessage = {User:string; Message:string}

let settings = 
    let settings = new JsonSerializerSettings()
    settings.TypeNameHandling <- TypeNameHandling.Auto
    settings

let serialize (event:'a)= 
    let serializedEvent = JsonConvert.SerializeObject(event, settings)
    let data = System.Text.Encoding.UTF8.GetBytes(serializedEvent)
    EventData(Guid.NewGuid(), typeof<ChatMessage>.Name, true, data, null)

let deserialize<'a> (event: EventStore.ClientAPI.ResolvedEvent) = 
    let serializedString = System.Text.Encoding.UTF8.GetString(event.Event.Data)
    let event = JsonConvert.DeserializeObject<'a>(serializedString, settings)
    event

let appendToStream (store:IEventStoreConnection) streamId newEvents =
    let serializedEvents = newEvents |> List.map serialize |> List.toArray
    store.AppendToStreamAsync(streamId, ExpectedVersion.Any, serializedEvents)


let initializeConnection = 
    let ipAddresses = Dns.GetHostAddresses("localhost")
    let ipadress = ipAddresses.[0]
    let endpoint = new IPEndPoint(ipadress, 1113)
    let settings = 
        let s = ConnectionSettings.Create()
                    .UseConsoleLogger()
                    .SetDefaultUserCredentials(new UserCredentials("admin", "changeit"))
                    .Build()
        s

    let connection = EventStoreConnection.Create(settings, endpoint)
    connection.Connected.Add(fun (args) -> printfn "-- Connected")
    connection.Disconnected.Add(fun (args) -> printfn "-- Disconnected")
    connection.ConnectAsync()
    printfn "connecting..."
    connection
  
let chatMessageRecieved (chatMessage:ChatMessage) =
    let text = String.Format("{0} says:\n{1}", chatMessage.User, chatMessage.Message)
    printfn "%s" text

let onRecieved event = 
    let chatMessage = deserialize<ChatMessage>(event)
    let text = chatMessage.User + " says:\n" + chatMessage.Message + ";"
    printfn "%s" text 

let rec listenMessage room connection =
    let message = Console.ReadLine()
    match message with
        |"!q" -> ignore
        | _ -> 
            appendToStream connection room [{User = "ouarzy"; Message = message}] 
            listenMessage room connection
    Thread.Sleep 1000
    listenMessage room connection



[<EntryPoint>]
let main argv = 
    let room = "FSharpRoom"
    let connection = initializeConnection
    connection.SubscribeToStreamAsync(room, false, new Action<EventStoreSubscription, EventStore.ClientAPI.ResolvedEvent>(fun((subscription : EventStore.ClientAPI.EventStoreSubscription)) -> (fun(event : EventStore.ClientAPI.ResolvedEvent) -> onRecieved(event)))) |> ignore
    
    listenMessage room connection |> ignore
    0
