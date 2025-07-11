using System;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Text;
using CommandLine;
using Dapper;
using Milvus.Client;
using Newtonsoft.Json;
using Npgsql;

namespace MyApp
{
    public class demofrs
    {
        public Guid Id { get; set; }
        public Guid TrackId { get; set; }
        public long ReceivedTime { get; set; }
        public float[] embedding { get; set; }
        public float detConf { get; set; }
        public float faceWeight { get; set; }

        public Guid VideoSourceId { get; set; }
    }


    internal class Program
    {
        public static string timelogTxt;
        private static int eventInsertedCount = 0;
        private static int eventUpdatedCount = 0;

        public static string Id = Guid.Empty.ToString();
        public static string MilvusIp = "localhost";

        public static string DbConnectionString =
            "User ID=postgres;Password=postgres;Host=localhost;Port=5438;Database=timescaledb;Pooling=true;Include Error Detail=true;";

        public static string EventCollectionName = "demoCollection";
        private static MilvusClient _milvusClient;
        public static MilvusCollection _milvusCollection;
        public static string deletionId;
        public static Dictionary<string, List<demofrs>> clusterInfo =
            new Dictionary<string, List<demofrs>>();
        private static float ConfidenceThreshold=0.9f;


        static async Task Main(string[] args)
        {
            string eventIdFileName = "last_event_id.txt";

            // Check if the file exists
            if (!File.Exists(eventIdFileName))
            {
                // Create the file and write text into it
                File.WriteAllText(eventIdFileName, Id);
                Console.WriteLine("File created and text written.");
            }
            else
            {
                Id = File.ReadAllText(eventIdFileName).Trim();
                Console.WriteLine($"File already exists. ID read from file: {Id}");
            }

            string connectionStringFileName = "connection_string.txt";

            // Check if the file exists
            if (!File.Exists(connectionStringFileName))
            {
                // Create the file and write text into it
                File.WriteAllText(connectionStringFileName, DbConnectionString);
                Console.WriteLine("File created and text written.");
            }
            else
            {
                DbConnectionString = File.ReadAllText(connectionStringFileName);
                Console.WriteLine($"File already exists. ID read from file: {DbConnectionString}");
            }

            string milvusIpFileName = "milvus_ip.txt";

            // Check if the file exists
            if (!File.Exists(milvusIpFileName))
            {
                // Create the file and write text into it
                File.WriteAllText(milvusIpFileName, MilvusIp);
                Console.WriteLine("File created and text written.");
            }
            else
            {
                MilvusIp = File.ReadAllText(milvusIpFileName);
                Console.WriteLine($"File already exists. ID read from file: {MilvusIp}");
            }
             timelogTxt = "timeLog.txt";

            // Check if the file exists
            if (!File.Exists(timelogTxt))
            {
                // Create the file and write text into it
                File.AppendAllTextAsync(timelogTxt, Id);
                Console.WriteLine("File created and text written.");
            }
           

            var startTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            try
            {
                _milvusClient = new MilvusClient(MilvusIp, port: 19530);
                //await _milvusClient.CreateDatabaseAsync("frs");
                // var databases = await _milvusClient.ListDatabasesAsync();
                //
                // foreach (var database in databases)
                // {
                //     Console.WriteLine(database);
                // }

                var schema = new CollectionSchema
                {
                    Fields =
                    {
                        FieldSchema.CreateVarchar("track_id", maxLength: 50, isPrimaryKey: true),
                        FieldSchema.CreateVarchar("event_id", maxLength: 50),
                        FieldSchema.Create<long>("event_time"),
                        FieldSchema.CreateFloatVector("embedding", dimension: 512),
                        FieldSchema.CreateVarchar("device_id", maxLength: 50)
                    }
                };

                if (!await _milvusClient.HasCollectionAsync(EventCollectionName))
                {
                    _milvusCollection = await _milvusClient.CreateCollectionAsync(EventCollectionName, schema,
                        consistencyLevel: ConsistencyLevel.Strong);
                }
                else
                {
                    _milvusCollection = _milvusClient.GetCollection(EventCollectionName);
                }

                var extraParams = new Dictionary<string, string> { { "M", "130" }, { "efConstruction", "660" } };
                await _milvusCollection.CreateIndexAsync("embedding", indexType: IndexType.Hnsw,
                    metricType: SimilarityMetricType.Ip,
                    extraParams: extraParams);
                // var extraParams = new Dictionary<string, string> {  { "nlist", "1024" }  };
                // await _milvusCollection.CreateIndexAsync("embedding", indexType: IndexType.Flat,
                //     metricType: SimilarityMetricType.Ip,
                //     extraParams: extraParams);
                 await _milvusCollection.LoadAsync();
                // Console.Write("Enter deletionId (GUID): ");
                // string deletionId = Console.ReadLine()?.Trim();
                //
                // if (Guid.TryParse(deletionId, out Guid parsedGuid))
                // {
                //     string filter = $"event_id <  \"{parsedGuid}\"";
                //     await _milvusCollection.DeleteAsync(filter);
                //     Console.WriteLine("Delete operation submitted.");
                // }
                // else
                // {
                //     Console.WriteLine("Invalid GUID format. Please enter a valid GUID.");
                // }
               //
               //  Console.WriteLine("Hello World! started milvusinsertion");
               //  Stopwatch stopwatch = Stopwatch.StartNew();
               //
               //  await ProcessEventsNotHavingGroupIds();
               //
               //  stopwatch.Stop();
               //
               //  var logstring = $"stopped milvusinsertion. Total time taken: {stopwatch.Elapsed}";
               //  Console.WriteLine(logstring);
               // File.AppendAllTextAsync(timelogTxt, logstring);
               //  File.WriteAllText(eventIdFileName, Id);
               //  await ProcessIndexWork();
         //       //
                 Stopwatch stopwatch = Stopwatch.StartNew();
           
               await EventProcessor.StartGroupIdWork(DbConnectionString);
           stopwatch.Stop();
                        
           var logstring = $"stopped groupidwork. Total time taken: {stopwatch.Elapsed}";
         Console.WriteLine(logstring);
         
          File.AppendAllTextAsync(timelogTxt, logstring);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            var endTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            Console.WriteLine($"TIME TAKEN {endTime - startTime}");
        }

        public static async Task ProcessIndexWork()
        {
            var response = await _milvusCollection.DescribeIndexAsync("embedding", "embedding");
            // Check the indexing status
            while (response[0].PendingIndexRows != 0)
            {
                response = await _milvusCollection.DescribeIndexAsync("embedding", "embedding");
                Console.WriteLine("rows left is " + response[0].PendingIndexRows);
                Console.WriteLine("index done is " + response[0].IndexedRows);
                Console.WriteLine("state is " + response[0].State);
                await Task.Delay(5000);
            }
        }

        public static async Task ProcessEventsNotHavingGroupIds()
        {
            NpgsqlConnection npgsqlConnection = new NpgsqlConnection(
                DbConnectionString
            );
            npgsqlConnection.Open();
            try
            {
                var processedEvent = 0;
                var limit = 1000;
                var offset = 0;

                while (true)
                {
                    Console.WriteLine("pereventInsertion Log");
                    Stopwatch stopwatch = Stopwatch.StartNew();

                    var getFaceEventsFromDbQuery = "";

                    getFaceEventsFromDbQuery =
                        $"select e.\"Id\",e.\"embedding\"::real[],e.\"TrackId\", e.\"ReceivedTime\",\"detConf\", \"faceWeight\", \"VideoSourceId\"  from events.\"Face_Recognition\" as e where e.\"Id\">'{Id}'  and e.\"detConf\" > {ConfidenceThreshold} and e.\"faceWeight\" > {ConfidenceThreshold}  ORDER BY e.\"Id\" FETCH NEXT ({limit}) ROWS ONLY;";


                    var events = npgsqlConnection
                        .Query<demofrs>(getFaceEventsFromDbQuery)
                        .ToList();
                    if (events.Count == 0)
                    {
                        break;
                    }

                    Dictionary<Guid, demofrs> eventsToBeinserted = new();
                    eventsToBeinserted = await GetEventsToBeInserted(events);

                    await AddEventsToQueueAsync(eventsToBeinserted);
                    offset = offset + limit;
                    processedEvent += events.Count;
                    Id = events.Last().Id.ToString();
                    stopwatch.Stop();
                    var logstring = "ProcessEventsNotHavingGroupIds Done : " + processedEvent + " Total time taken: " + stopwatch.Elapsed;
                    Console.WriteLine(logstring);
                    File.AppendAllTextAsync(timelogTxt, logstring);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            npgsqlConnection.Close();
        }

        public static async Task<Dictionary<Guid, demofrs>> GetEventsToBeInserted(List<demofrs> events)
        {
            try
            {
               
                if (!events.Any())
                    return new Dictionary<Guid, demofrs>();

                var embeddings = events
                    .Select(e => new ReadOnlyMemory<float>(e.embedding.ToArray()))
                    .ToList();

                var parameters = new SearchParameters
                {
                    OutputFields = { "track_id", "event_id", "event_time" },
                    ConsistencyLevel = ConsistencyLevel.Strong,
                    Offset = 0,
                    ExtraParameters = { ["ef"] = "130" },
                };

                var searchResults = await Program._milvusCollection.SearchAsync(
                    EventProcessor.EventCollectionProperties.Embedding,
                    embeddings,
                    SimilarityMetricType.Ip,
                    limit: 1,
                    parameters
                );

                var toInsert = new Dictionary<Guid, demofrs>();
                for (int i = 0; i < events.Count; i++)
                {
                    if (ShouldInsertEvent(searchResults, i))
                    {
                        toInsert.TryAdd(events[i].TrackId, events[i]);
                    }
                }

                var insert = PostProcessInsertion(toInsert);
                return insert;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return null;
        }
    
        // private static Dictionary<Guid, demofrs> PostProcessInsertion(Dictionary<Guid, demofrs> toInsert)
        // {
        //     if (!toInsert.Any())
        //         return toInsert;
        //     
        //     var insert = new Dictionary<Guid, demofrs>();
        //     var keys = toInsert.Keys.ToList(); 
        //     int count = keys.Count;
        //     var isInserted = new bool[count];  
        //
        //     for (int i = 0; i < count; i++)
        //     {
        //         if (isInserted[i])
        //             continue;
        //
        //         var keyI = keys[i];
        //         var embeddingI = toInsert[keyI].embedding;
        //
        //         for (int j = i + 1; j < count; j++) 
        //         {
        //             var keyJ = keys[j];
        //             var embeddingJ = toInsert[keyJ].embedding;
        //
        //             var cosineSimilarity = EventProcessor.InnerProduct(embeddingI, embeddingJ);
        //             if (cosineSimilarity > 0.5)
        //             {
        //                 isInserted[j] = true;
        //             }
        //         }
        //
        //         
        //         insert.TryAdd(toInsert[keyI].TrackId, toInsert[keyI]);
        //     }
        //
        //     return insert;
        // }
        
        private static Dictionary<Guid, demofrs> PostProcessInsertion(Dictionary<Guid, demofrs> toInsert)
        {
            if (!toInsert.Any())
                return toInsert;
            
           

            

            var insert = new Dictionary<Guid, demofrs>();
            var clusters = new List<ReadOnlyMemory<float>>();

            foreach (var kvp in toInsert)
            {
                var currentEmbedding = kvp.Value.embedding;
                bool matched = false;

                for (int i = 0; i < clusters.Count; i++)
                {
                    var cosineSimilarity = EventProcessor.InnerProduct(currentEmbedding, clusters[i]);
                    if (cosineSimilarity > 0.5)
                    {
                        matched = true;
                        break;
                    }
                }

                if (!matched)
                {
                    clusters.Add(currentEmbedding);
                    insert.TryAdd(kvp.Value.TrackId, kvp.Value);

                }
            }
            

            return insert;
        }



        private static bool ShouldInsertEvent(SearchResults searchResults, int index)
        {
            const float similarityThreshold = 0.5f;
    
            if (searchResults is null || searchResults.Scores.Count < 1)
            {
                return true;
            }

            return searchResults.Scores[index] < similarityThreshold;
        }
        

        public static async Task<bool> AddEventsToQueueAsync(Dictionary<Guid, demofrs> eventsToBeinserted)
        {
            try
            {
                if (eventsToBeinserted.Count == 0)
                {
                    return true;
                }

                // var trackIdSet = new HashSet<string>();
                // foreach (var se in paramsgh.TrackId)
                // {
                //     if (trackIdSet.Contains(se))
                //     {
                //         Console.WriteLine("error detected");
                //     }
                //
                //     trackIdSet.Add(se);
                // }
                var events = eventsToBeinserted.Values.ToList();
                List<ReadOnlyMemory<float>> embeddings = events
                    .Select(e => new ReadOnlyMemory<float>(e.embedding))
                    .ToList();
                var x = await _milvusCollection.UpsertAsync(new FieldData[]
                {
                    FieldData.Create($"{EventProcessor.EventCollectionProperties.TrackId}",
                        events.Select(x => x.TrackId.ToString()).ToList()),
                    FieldData.Create($"{EventProcessor.EventCollectionProperties.EventId}",
                        events.Select(x => x.Id.ToString()).ToList()),
                    FieldData.Create<long>($"{EventProcessor.EventCollectionProperties.EventTime}",
                        events.Select(x => x.ReceivedTime).ToList()),

                    FieldData.CreateFloatVector($"{EventProcessor.EventCollectionProperties.Embedding}", embeddings),
                    FieldData.Create($"{EventProcessor.EventCollectionProperties.VideoSourceId}",
                        events.Select(x => x.VideoSourceId.ToString()).ToList()),
                });
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
    }
}