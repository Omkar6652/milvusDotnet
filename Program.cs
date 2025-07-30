using System.Diagnostics;
using Dapper;
using Grpc.Net.Client;
using Npgsql;
using Qdrant.Client;
using Qdrant.Client.Grpc;
using Range = Qdrant.Client.Grpc.Range;

namespace MyApp
{
    public class demofrs
    {
        public Guid Id { get; set; }
        public Guid TrackId { get; set; }
        public long Time { get; set; }
        public float[] embedding { get; set; }
        public Guid VideoSourceId { get; set; }
    }

    internal class Program
    {
        public static string timelogTxt = "timeLog.txt";
        public static string groupIdTimeLogText = "groupIdTimeLog.txt";
        private static int eventInsertedCount = 0;
        private static int eventUpdatedCount = 0;
        public static string Id = Guid.Empty.ToString();
        public static string QdrantIp = "localhost";

        public static string DbConnectionString =
            "User ID=postgres;Password=postgres;Host=localhost;Port=5438;Database=timescaledb;Pooling=true;";

        public static string EventCollectionName = "demoCollection";

        public static QdrantClient _qdrantClient;
        public static string deletionId;

        public static Dictionary<string, List<demofrs>> clusterInfo = new();

        // 🔧 Configurable Parameters
        public static int M = 130;
        public static int EfConstruct = 660;
        public static int Ef = 130;
        public static float SimilarityThreshold = 0.5f; // For upsert logic

        static async Task Main(string[] args)
        {
            LoadConfig("last_event_id.txt", ref Id);
            LoadConfig("connection_string.txt", ref DbConnectionString);
            LoadConfig("milvus_ip.txt", ref QdrantIp);

            if (!File.Exists(timelogTxt)) File.WriteAllText(timelogTxt, "");
            if (!File.Exists(groupIdTimeLogText)) File.WriteAllText(groupIdTimeLogText, "");

            try
            {
                _qdrantClient = new QdrantClient(QdrantIp, 6334 /*, apiKey: qdrantApiKey if supported directly */ /*, channelOptions if needed directly */);

                // ✅ Create collection if not exists
                if (!await _qdrantClient.CollectionExistsAsync(EventCollectionName))
                {
                    await _qdrantClient.CreateCollectionAsync(
                        EventCollectionName,
                        new VectorParams
                        {
                            Size = (ulong)512,
                            Distance = Distance.Cosine
                        });

                    Console.WriteLine("✅ Collection created with HNSW.");
                }

                // ✅ Wait for collection to be ready
                while ((await _qdrantClient.GetCollectionInfoAsync(EventCollectionName)).Status !=
                       CollectionStatus.Green)
                {
                    Console.WriteLine("🟡 Waiting for collection to be ready...");
                    await Task.Delay(1000);
                }

                Console.WriteLine("🚀 Hello World! Started qdrant insertion");
                var stopwatch = Stopwatch.StartNew();
                await ProcessEventsNotHavingGroupIds();
                stopwatch.Stop();
                
                var log = $"⏹️ Stopped qdrant insertion. Total time: {stopwatch.Elapsed}";
                Console.WriteLine(log);
                await File.AppendAllTextAsync(timelogTxt, log + Environment.NewLine);
              
              //  Save last processed ID
               await File.WriteAllTextAsync("last_event_id.txt", Id);
              
               await ProcessIndexWork();
                      Stopwatch stopswatch = Stopwatch.StartNew();
                  
                    await EventProcessor.StartGroupIdWork(DbConnectionString);
                    stopswatch.Stop();
                               
                var groupidLogString = $"stopped groupidwork. Total time taken: {stopswatch.Elapsed}";
                Console.WriteLine(groupidLogString);
                
                 File.AppendAllTextAsync(groupIdTimeLogText, groupidLogString);
            }
            catch (Exception ex)
            {
                Console.WriteLine("❌ Error: " + ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        private static void LoadConfig(string fileName, ref string value)
        {
            if (!File.Exists(fileName))
            {
                File.WriteAllText(fileName, value);
                Console.WriteLine($"📄 Created {fileName}: {value}");
            }
            else
            {
                value = File.ReadAllText(fileName).Trim();
                Console.WriteLine($"📄 Loaded from {fileName}: {value}");
            }
        }

        public static async Task ProcessIndexWork()
        {
            var info = await _qdrantClient.GetCollectionInfoAsync(EventCollectionName);
            while (info?.IndexedVectorsCount < info?.VectorsCount)
            {
                info = await _qdrantClient.GetCollectionInfoAsync(EventCollectionName);
                Console.WriteLine($"🔍 Indexing... {info.IndexedVectorsCount}/{info.VectorsCount}");
                await Task.Delay(2000);
            }

            Console.WriteLine("✅ Indexing complete.");
        }

        public static async Task ProcessEventsNotHavingGroupIds()
        {
            using var conn = new NpgsqlConnection(DbConnectionString);
            conn.Open();

            var limit = 1000;
            var processed = 0;

            while (true)
            {
                var query = $@"
                    SELECT ""Id"", ""embedding""::real[], ""TrackId"", ""Time"", ""VideoSourceId""
                    FROM events.""Face_Recognition""
                    WHERE ""Id"" > '{Id}'
                    ORDER BY ""Id"" ASC
                    LIMIT {limit};";

                var events = conn.Query<demofrs>(query).ToList();
                if (events.Count == 0) break;

                var toInsert = await GetEventsToBeInserted(events);
                await AddEventsToQueueAsync(toInsert);

                Id = events.Last().Id.ToString();
                processed += events.Count;

                Console.WriteLine($"✅ Processed {processed} events");
            }

            conn.Close();
        }

public static async Task<Dictionary<Guid, demofrs>> GetEventsToBeInserted(List<demofrs> events)
{
    if (!events.Any()) return new Dictionary<Guid, demofrs>();

    try
    {
        // 1. Prepare the list of SearchPoints requests for batch search
//         var searchPointsList = new List<SearchPoints>();
//
//         foreach (var e in events)
//         {
//             var searchRequest = new SearchPoints
//             {
//                 CollectionName = EventCollectionName, // Set collection name for each request
//                 Vector = { e.embedding }, // RepeatedField<float> accepts IEnumerable<float>
//                 Limit = 1,
//                 WithPayload = false, // We only need the score to decide whether to insert
//                 // WithVectors = false, // Add if needed, but not required here
//                 Params = new SearchParams { HnswEf = (ulong)Ef }, // Set HNSW ef parameter
//                 // Create the Filter object for event_time < e.Time
//                 Filter = new Filter
//                 {
//                     // Add conditions to the 'Must' list for AND logic
//                     Must = {
//                         new Condition
//                         {
//                             Field = new FieldCondition
//                             {
//                                 // IMPORTANT: In Qdrant gRPC filters, payload keys are referenced directly
//                                 // without the "payload." prefix.
//                                 Key = "event_time", // Use the payload key directly
//                                 Range = new Range { Lt = (double)e.Time } // Lt for Less Than (use double)
//                             }
//                         }
//                         // Add more conditions to 'Must' if needed
//                     }
//                     // Use 'Should' for OR logic, 'MustNot' for NOT logic if needed
//                 }
//                 // Offset = 0, // Default is 0
//                 // ScoreThreshold = ... // Optional: Set a minimum score threshold if needed
//             };
//
//             searchPointsList.Add(searchRequest);
//         }
//
//     var seq=    Stopwatch.StartNew();
//         // 2. Execute the batch search using the QdrantClient instance method
//         var batchResults = await _qdrantClient.SearchBatchAsync(
//             collectionName: EventCollectionName, // Main collection name (often set per request too)
//             searches: searchPointsList, // The list of SearchPoints requests
//             readConsistency: null, // Optional: ReadConsistency
//             timeout: null, // Optional: Timeout
//             cancellationToken: default // Optional: Cancellation Token
//         );
// seq.Stop();
// Console.WriteLine(seq.Elapsed + ": search time for batch");
//         // 3. Process the batch results
        var toInsert = new Dictionary<Guid, demofrs>();
       
        // for (int i = 0; i < batchResults.Count; i++)
        // {
        //     var batchResult = batchResults[i]; // BatchResult for events[i]
        //     var currentEvent = events[i];       // The original event
        //
        //     // Check if any results were returned for this specific embedding's search
        //     // and if the best score meets or exceeds the threshold.
        //     if (batchResult.Result.Count == 0 || batchResult.Result[0].Score < SimilarityThreshold)
        //     {
        //         // Case 1: No results found, or
        //         // Case 2: Best result's score is below the threshold.
        //         // In either case, consider it a new, unique item and mark for insertion.
        //         toInsert[currentEvent.TrackId] = currentEvent;
        //         //Console.WriteLine($"[Batch] Marking event {currentEvent.Id} for insertion (Score: {(batchResult.Result.Count > 0 ? batchResult.Result[0].Score : -1.0f)})");
        //     }
        //     // else
        //     // {
        //     //     // Case 3: A result was found with a score >= threshold.
        //     //     // It's considered a duplicate, so we don't add it to 'toInsert'.
        //     //     //Console.WriteLine($"[Batch] Skipping event {currentEvent.Id} (Score: {batchResult.Result[0].Score})");
        //     // }
        // }
//to comment from here tp
        
        
        foreach (var e in events)
        {
            toInsert[e.TrackId] = e;
        }
        return toInsert;
        //here
        // 4. Apply the final PostProcessInsertion step
        return PostProcessInsertion(toInsert);

    }
    catch (Exception ex) // Catch specific gRPC exceptions (Grpc.Core.RpcException) if needed
    {
        Console.WriteLine($"Error during Qdrant batch search in GetEventsToBeInserted: {ex.Message}");
        

        throw; // Re-throw to maintain original error handling flow
    }
}
        private static Dictionary<Guid, demofrs> PostProcessInsertion(Dictionary<Guid, demofrs> toInsert)
        {
            if (!toInsert.Any()) return toInsert;

            var insert = new Dictionary<Guid, demofrs>();
            var clusters = new List<float[]>();

            foreach (var kvp in toInsert)
            {
                var current = kvp.Value.embedding;
                bool matched = false;

                foreach (var cluster in clusters)
                {
                    var sim = EventProcessor.InnerProduct(current, cluster);
                    if (sim > 0.5)
                    {
                        matched = true;
                        break;
                    }
                }

                if (!matched)
                {
                    clusters.Add(current);
                    insert[kvp.Key] = kvp.Value;
                }
            }

            return insert;
        }

        public static async Task<bool> AddEventsToQueueAsync(Dictionary<Guid, demofrs> eventsToBeInserted)
        {
            if (eventsToBeInserted.Count == 0) return true;

            var points = eventsToBeInserted.Values.Select(e => new PointStruct
            {
                Id = new PointId { Uuid = e.TrackId.ToString() }, // Use TrackId as ID
                Vectors = e.embedding,
                Payload =
                {
                    ["track_id"] = e.TrackId.ToString(),
                    ["event_id"] = e.Id.ToString(),
                    ["device_id"] = e.VideoSourceId.ToString(),
                    ["time"] = e.Time
                }
            }).ToList();

            await _qdrantClient.UpsertAsync(EventCollectionName, points);
            eventInsertedCount += points.Count;
            Console.WriteLine($"📤 Upserted {points.Count} vectors");
            return true;
        }
    }
}