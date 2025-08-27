using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Milvus.Client;
using MyApp;
using MyApp.Models;
using Newtonsoft.Json;
using Npgsql;
using Serilog;

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
        public static string timelogTxt;
        public static string groupIdTimeLogText;
        private static int eventInsertedCount = 0;
        private static int eventUpdatedCount = 0;

        public static string Id = Guid.Empty.ToString();
        public static string MilvusIp = "localhost";

        public static string DbConnectionString =
            "User ID=postgres;Password=postgres;Host=localhost;Port=5438;Database=timescaledb;Pooling=true;Include Error Detail=true;";

        public static string EventCollectionName = "demoCollection";
        
        // Separate Milvus clients for each collection
        public static MilvusClient _watchlistMilvusClient;
        public static MilvusClient _eventMilvusClient;
        public static MilvusClient _uniquePeopleMilvusClient;
        
        // Separate Milvus collections
        public static MilvusCollection _watchlistMilvusCollection;
        public static MilvusCollection _eventMilvusCollection;
        public static MilvusCollection _uniquePeopleMilvusCollection;
        
        public static string deletionId;
        public static Dictionary<string, List<demofrs>> clusterInfo =
            new Dictionary<string, List<demofrs>>();

        static async Task Main(string[] args)
        {
           
            string eventIdFileName = "last_event_id.txt";

            // Check if the file exists
            if (!File.Exists(eventIdFileName))
            {
                File.WriteAllText(eventIdFileName, Id);
                Console.WriteLine("File created and text written.");
            }
            else
            {
                Id = File.ReadAllText(eventIdFileName).Trim();
                Console.WriteLine($"File already exists. ID read from file: {Id}");
            }

            string connectionStringFileName = "connection_string.txt";

            if (!File.Exists(connectionStringFileName))
            {
                File.WriteAllText(connectionStringFileName, DbConnectionString);
                Console.WriteLine("File created and text written.");
            }
            else
            {
                DbConnectionString = File.ReadAllText(connectionStringFileName);
                Console.WriteLine($"File already exists. Connection string read from file: {DbConnectionString}");
            }

            string milvusIpFileName = "milvus_ip.txt";

            if (!File.Exists(milvusIpFileName))
            {
                File.WriteAllText(milvusIpFileName, MilvusIp);
                Console.WriteLine("File created and text written.");
            }
            else
            {
                MilvusIp = File.ReadAllText(milvusIpFileName);
                Console.WriteLine($"File already exists. Milvus IP read from file: {MilvusIp}");
            }
            
            timelogTxt = "timeLog.txt";
            groupIdTimeLogText = "groupIdTimeLog.txt";

            if (!File.Exists(timelogTxt))
            {
                File.AppendAllTextAsync(timelogTxt, Id);
                Console.WriteLine("File created and text written.");
            }
            if (!File.Exists(groupIdTimeLogText))
            {
                File.AppendAllTextAsync(groupIdTimeLogText, Id);
                Console.WriteLine("File created and text written.");
            }

            var startTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            try
            {
                Task.Factory.StartNew(
                    () =>
                    {
                        while (true)
                        {
                            var now = DateTime.Now;

                            if (now.Hour == 2)
                            {
                                try
                                {
                                    string time = GetRemoveLogsAfter();
                                    if (string.IsNullOrEmpty(time))
                                    {
                                        time = "30";
                                    }

                                    var checkTime = DateTimeOffset
                                        .UtcNow.AddDays(-Int32.Parse(time))
                                        .ToUnixTimeMilliseconds();

                                    cleanLogs(checkTime);
                                }
                                catch (Exception ex)
                                {
                                    Log.Error($"Error during LogsCleaner execution: {ex.Message}");
                                }
                            }

                            Task.Delay(TimeSpan.FromMinutes(15)).Wait();
                        }
                    },
                    TaskCreationOptions.LongRunning
                );
                PrepareMilvus();
                await RemoveTriggersAndIndexing();
    
                Console.WriteLine("Hello World! started milvusinsertion");
                Stopwatch stopwatch = Stopwatch.StartNew();
                await ProcessWatchListCollectionInsertion();
                await ProcessEventsAndUniquePeopleInsertion();
               
                stopwatch.Stop();
               
                var logstring = $"stopped milvusinsertion. Total time taken: {stopwatch.Elapsed}";
                Console.WriteLine(logstring);
                File.AppendAllTextAsync(timelogTxt, logstring);
                File.WriteAllText(eventIdFileName, Id);
                // await ProcessIndexWork(); 
                Stopwatch stopswatch = Stopwatch.StartNew();
           
                await EventProcessor.StartGroupIdAssignWork(DbConnectionString);
                stopswatch.Stop();
                        
                var groupidLogString = $"stopped groupidwork. Total time taken: {stopswatch.Elapsed}";
                Console.WriteLine(groupidLogString);
         
                File.AppendAllTextAsync(groupIdTimeLogText, groupidLogString);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            var endTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            Console.WriteLine($"TIME TAKEN {endTime - startTime}");
        }

        private static async Task RemoveTriggersAndIndexing()
        {
            using var npgsqlConnection = new NpgsqlConnection(DbConnectionString);
            try
            {
                await npgsqlConnection.OpenAsync();

                // Drop existing objects if they exist
                string dropQuery = @"
            -- Drop the trigger if it exists
            DROP TRIGGER IF EXISTS before_insert_face_recognition ON events.""Face_Recognition"";

            -- Drop the function if it exists
            DROP FUNCTION IF EXISTS events.assign_group_id_to_frs_events();

            -- Drop the index if it exists
            DROP INDEX IF EXISTS ""FacePoint_embedding_idx"";";

                await npgsqlConnection.ExecuteAsync(dropQuery);
            }
            catch (Exception ex)
            {
                // Log the exception or handle it as needed
                Console.WriteLine($"Error removing triggers and indexing: {ex.Message}");
                throw; // Re-throw if you want calling code to handle it
            }
            finally
            {
                if (npgsqlConnection.State == System.Data.ConnectionState.Open)
                {
                    npgsqlConnection.Close();
                }
            }
        }

        private static string GetRemoveLogsAfter()
        {
            NpgsqlConnection npgsqlConnection = new NpgsqlConnection(DbConnectionString);
            npgsqlConnection.Open();
           var query = "SELECT \"Value\" FROM public.\"SystemConfig\" where  \"Key\" = 'RemoveEventLogsAfter'" ;


            var watchListItems = npgsqlConnection
                .Query<string>(query)
                .ToList();
            return watchListItems[0];
        }

        private static async Task ProcessWatchListCollectionInsertion()
{
    NpgsqlConnection npgsqlConnection = new NpgsqlConnection(DbConnectionString);
    npgsqlConnection.Open();
    
    try
    {
        var processedCount = 0;
        var limit = 1000;
        var offset = 0;

        while (true)
        {
            Console.WriteLine("Processing WatchList Collection Insertion...");
            Stopwatch stopwatch = Stopwatch.StartNew();

            var query = $@"
                SELECT ""Id"" as FaceId, ""PersonId"", ""Embedding""::real[] as Embeddings 
                FROM public.""FacePoint"" 
                ORDER BY ""Id"" 
                OFFSET {offset} 
                LIMIT {limit}";

            var watchListItems = npgsqlConnection
                .Query<WatchListCollectionModel>(query)
                .ToList();

            if (watchListItems.Count == 0)
            {
                break;
            }

            await InsertWatchListItems(watchListItems);
            
            offset += limit;
            processedCount += watchListItems.Count;
            
            stopwatch.Stop();
            var logstring = $"WatchList Insertion Done: {processedCount} Total time taken: {stopwatch.Elapsed}";
            Console.WriteLine(logstring);
            File.AppendAllTextAsync(timelogTxt, logstring);
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error in ProcessWatchListCollectionInsertion: {ex.Message}");
        Log.Error($"Error in ProcessWatchListCollectionInsertion: {ex.Message}");
    }
    finally
    {
        npgsqlConnection.Close();
    }
}

private static async Task<bool> InsertWatchListItems(List<WatchListCollectionModel> items)
{
    try
    {
        if (items.Count == 0)
        {
            return true;
        }

        List<ReadOnlyMemory<float>> embeddings = items
            .Select(e => new ReadOnlyMemory<float>(e.Embeddings.ToArray()))
            .ToList();

        var result = await _watchlistMilvusCollection.UpsertAsync(new FieldData[]
        {
            FieldData.Create(WatchListCollectionProperties.FaceId,
                items.Select(x => x.FaceId.ToString()).ToList()),
            FieldData.Create(WatchListCollectionProperties.PersonId,
                items.Select(x => x.PersonId.ToString()).ToList()),
            FieldData.CreateFloatVector(WatchListCollectionProperties.Embedding, embeddings)
        });

        Console.WriteLine($"Inserted {items.Count} items into WatchList collection");
        return true;
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error inserting WatchList items: {ex.Message}");
        Log.Error($"Error inserting WatchList items: {ex.Message}");
        return false;
    }
}

        public class MilvusDbInfoParameters
        {
            public string CollectionName { get; set; }
            public CollectionSchema CollectionSchema { get; set; }
            public MilvusDbIndexParameters MilvusDbIndexParameters { get; set; }
        }

        public class MilvusDbIndexParameters
        {
            public string FieldName { get; set; }
            public IndexType IndexType { get; set; }
            public SimilarityMetricType SimilarityMetricType { get; set; }
            public Dictionary<string, string> ExtraParams { get; set; }
        }

        public static async Task<bool> IsVectorConnectedAsync(MilvusClient client)
        {
            if (client == null)
            {
                return false;
            }

            try
            {
                var res = await client.HealthAsync();
                return res.IsHealthy;
            }
            catch (Exception ex)
            {
                Log.Error($"Error getting health: {ex.Message}");
                return false;
            }
        }

        public static async Task PrepareMilvusDb(MilvusClient client, MilvusDbInfoParameters milvusDbInfoParameters)
        {
            try
            {
                const int retryDelayMs = 2000;
                int attempt = 0;

                while (true)
                {
                    attempt++;
                    if (await IsVectorConnectedAsync(client))
                    {
                        Log.Error($"Successfully connected to Milvus DB on attempt {attempt}");
                        break;
                    }
    
                    Log.Error($"Milvus DB connection attempt {attempt} failed. Retrying in {retryDelayMs}ms...");
                    await Task.Delay(retryDelayMs);
                }

                if (!await client.HasCollectionAsync(milvusDbInfoParameters.CollectionName))
                {
                    var collection = await client.CreateCollectionAsync(milvusDbInfoParameters.CollectionName,
                        milvusDbInfoParameters.CollectionSchema, consistencyLevel: ConsistencyLevel.Strong);
                    
                    await collection.CreateIndexAsync(milvusDbInfoParameters.MilvusDbIndexParameters.FieldName,
                        indexType: milvusDbInfoParameters.MilvusDbIndexParameters.IndexType,
                        metricType: milvusDbInfoParameters.MilvusDbIndexParameters.SimilarityMetricType,
                        extraParams: milvusDbInfoParameters.MilvusDbIndexParameters.ExtraParams);

                    Log.Information($"Created new Milvus collection: {milvusDbInfoParameters.CollectionName}");
                }
                else
                {
                    Log.Information($"Connected to existing Milvus collection: {milvusDbInfoParameters.CollectionName}");
                }

                var milvusCollection = client.GetCollection(milvusDbInfoParameters.CollectionName);
                await milvusCollection.LoadAsync();
                
                // Assign to appropriate collection based on name
                switch (milvusDbInfoParameters.CollectionName)
                {
                    case enrollmentCollectionName:
                        _watchlistMilvusCollection = milvusCollection;
                        break;
                    case eventCollectionName:
                        _eventMilvusCollection = milvusCollection;
                        break;
                    case uniquePeopleCollection:
                        _uniquePeopleMilvusCollection = milvusCollection;
                        break;
                }
            }
            catch (Exception e)
            {
                Log.Error($"Error in [PrepareMilvusDb] of [MilvusWrapper]: {e.Message}");
                return;
            }
        }

        public const string enrollmentCollectionName = "watchlist";
        public const string eventCollectionName = "eventcollection";
        public const string uniquePeopleCollection = "uniquePeopleCollection";

        private class WatchListCollectionProperties
        {
            public const string FaceId = "faceId";
            public const string PersonId = "personId";
            public const string Embedding = "embedding";
        }

        private static void PrepareMilvus()
        {
            // Initialize separate Milvus clients
            _watchlistMilvusClient = new MilvusClient(MilvusIp, 19530);
            _eventMilvusClient = new MilvusClient(MilvusIp, 19530);
            _uniquePeopleMilvusClient = new MilvusClient(MilvusIp, 19530);

            var uniquePeopleCollectionSchema = new CollectionSchema
            {
                Fields =
                {
                    FieldSchema.CreateVarchar(EventProcessor.EventCollectionProperties.TrackId, maxLength: 50, isPrimaryKey: true),
                    FieldSchema.CreateVarchar(EventProcessor.EventCollectionProperties.EventId, maxLength: 50),
                    FieldSchema.CreateFloatVector(EventProcessor.EventCollectionProperties.Embedding, dimension: 512),
                }
            };

            var eventCollectionSchema = new CollectionSchema
            {
                Fields =
                {
                    FieldSchema.CreateVarchar(EventProcessor.EventCollectionProperties.TrackId, maxLength: 50, isPrimaryKey: true),
                    FieldSchema.Create<long>(EventProcessor.EventCollectionProperties.EventTime),
                    FieldSchema.CreateVarchar(EventProcessor.EventCollectionProperties.EventId, maxLength: 50),
                    FieldSchema.CreateFloatVector(EventProcessor.EventCollectionProperties.Embedding, dimension: 512),
                }
            };

            var schema = new CollectionSchema
            {
                Fields =
                {
                    FieldSchema.CreateVarchar(WatchListCollectionProperties.FaceId, maxLength: 50, isPrimaryKey: true),
                    FieldSchema.CreateVarchar(WatchListCollectionProperties.PersonId, maxLength: 50),
                    FieldSchema.CreateFloatVector(WatchListCollectionProperties.Embedding, dimension: 512)
                }
            };

            var watchListMilvusParams = new MilvusDbInfoParameters()
            {
                CollectionName = enrollmentCollectionName,
                CollectionSchema = schema,
                MilvusDbIndexParameters = new MilvusDbIndexParameters()
                {
                    FieldName = WatchListCollectionProperties.Embedding,
                    IndexType = IndexType.Hnsw,
                    SimilarityMetricType = SimilarityMetricType.Ip,
                    ExtraParams = new Dictionary<string, string>() { { "M", "30" }, { "efConstruction", "360" } }
                }
            };

            var milvusParams = new MilvusDbInfoParameters()
            {
                CollectionName = eventCollectionName,
                CollectionSchema = eventCollectionSchema,
                MilvusDbIndexParameters = new MilvusDbIndexParameters()
                {
                    FieldName = EventProcessor.EventCollectionProperties.Embedding,
                    IndexType = IndexType.Flat,
                    SimilarityMetricType = SimilarityMetricType.Ip,
                    ExtraParams = new Dictionary<string, string>() { }
                }
            };

            var milvusUniquePeopleParams = new MilvusDbInfoParameters()
            {
                CollectionName = uniquePeopleCollection,
                CollectionSchema = uniquePeopleCollectionSchema,
                MilvusDbIndexParameters = new MilvusDbIndexParameters()
                {
                    FieldName = EventProcessor.EventCollectionProperties.Embedding,
                    IndexType = IndexType.Hnsw,
                    SimilarityMetricType = SimilarityMetricType.Ip,
                    ExtraParams = new Dictionary<string, string>() { { "M", "30" }, { "efConstruction", "360" } }
                }
            };

            PrepareMilvusDb(_watchlistMilvusClient, watchListMilvusParams).GetAwaiter().GetResult();
            PrepareMilvusDb(_uniquePeopleMilvusClient, milvusUniquePeopleParams).GetAwaiter().GetResult();
            PrepareMilvusDb(_eventMilvusClient, milvusParams).GetAwaiter().GetResult();
        }

        public static async Task ProcessIndexWork()
        {
            var response = await _eventMilvusCollection.DescribeIndexAsync("embedding",EventProcessor.EventCollectionProperties.Embedding);
            while (response[0].PendingIndexRows != 0)
            {
                response = await _eventMilvusCollection.DescribeIndexAsync("embedding", EventProcessor.EventCollectionProperties.Embedding);
                Console.WriteLine("rows left is " + response[0].PendingIndexRows);
                Console.WriteLine("index done is " + response[0].IndexedRows);
                Console.WriteLine("state is " + response[0].State);
                await Task.Delay(5000);
            }
        }

        public static async Task ProcessEventsAndUniquePeopleInsertion()
        {
            NpgsqlConnection npgsqlConnection = new NpgsqlConnection(DbConnectionString);
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

                    var getFaceEventsFromDbQuery =
                        $"select e.\"Id\",e.\"embedding\"::real[],e.\"TrackId\", e.\"Time\", e.\"VideoSourceId\"  from events.\"Face_Recognition\" as e where e.\"Id\">'{Id}'    ORDER BY e.\"Id\" FETCH NEXT ({limit}) ROWS ONLY;";

                    var events = npgsqlConnection
                        .Query<demofrs>(getFaceEventsFromDbQuery)
                        .ToList();
                    
                    if (events.Count == 0)
                    {
                        break;
                    }
                    foreach (var evt in events)
                    {
                        if (evt.Time <= 0)
                        {
                            Console.WriteLine($"Warning: Event {evt.Id} has invalid time: {evt.Time}");
                        }
                        if (evt.embedding == null || evt.embedding.Length == 0)
                        {
                            Console.WriteLine($"Warning: Event {evt.Id} has null or empty embedding");
                        }
                    }

                    Dictionary<Guid, demofrs> eventsToBeinserted = new();
                    Dictionary<Guid, demofrs> uniqueEventsToBeInserted = new();
                    eventsToBeinserted = await GetEventsToBeInserted(events);
                    uniqueEventsToBeInserted = await GetUniqueEventsToBeInserted(events);

                    await AddEventsToQueueAsync(eventsToBeinserted);
                    await AddEventsToUniquePeopleQueueAsyc(uniqueEventsToBeInserted, npgsqlConnection);
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

        private static async Task AddEventsToUniquePeopleQueueAsyc(Dictionary<Guid, demofrs> uniqueEventsToBeInserted, NpgsqlConnection npgsqlConnection)
        {
            try
            {
                if (uniqueEventsToBeInserted.Count == 0)
                {
                    return ;
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
                var events = uniqueEventsToBeInserted.Values.ToList();
                List<ReadOnlyMemory<float>> embeddings = events
                    .Select(e => new ReadOnlyMemory<float>(e.embedding))
                    .ToList();
                
                var x = await _uniquePeopleMilvusCollection.UpsertAsync(new FieldData[]
                {
                    FieldData.Create($"{EventProcessor.EventCollectionProperties.TrackId}",
                        events.Select(x => x.TrackId.ToString()).ToList()),
                    FieldData.Create($"{EventProcessor.EventCollectionProperties.EventId}",
                        events.Select(x => x.Id.ToString()).ToList()),
                    FieldData.CreateFloatVector($"{EventProcessor.EventCollectionProperties.Embedding}", embeddings),
                   
                });
                await UpdateEventsBaseImageParam(x,npgsqlConnection);
                return ;
            }
            catch (Exception ex)
            {
                Log.Error(ex.Message);
                return ;
            }
            
        }
        private static async Task UpdateEventsBaseImageParam(MutationResult result, NpgsqlConnection npgsqlConnection)
        {
            var tableName = "events.\"Face_Recognition\""; // escaping schema/table properly
            var stringIds = result.Ids.StringIds;
            if (stringIds is null)
            {
                return;
            }


// Prepare values clause for input TrackIds
            var valuesClause = string.Join(", ", stringIds.Select(id => $"('{id.Replace("'", "''")}')"));

// For the WHERE clause in UPDATE
            var formattedIds = string.Join(", ", stringIds.Select(id => $"'{id.Replace("'", "''")}'"));

            var query = $@"
WITH updated AS (
  UPDATE {tableName}
  SET ""isBaseImage"" = TRUE
  WHERE ""TrackId"" IN ({formattedIds}) -- this should be a list of UUIDs in quotes
  RETURNING ""TrackId""
),
input_ids(""TrackId"") AS (
  VALUES {valuesClause} -- each value should be like ('uuid-value'::uuid)
)
SELECT
  i.""TrackId""
FROM input_ids i
LEFT JOIN updated u ON i.""TrackId"" = u.""TrackId"" :: text
WHERE u.""TrackId"" IS NULL;
";
            List<string> notFoundTrackIds;
         
                notFoundTrackIds = npgsqlConnection.Query<string>(query).ToList();
            
            if (notFoundTrackIds.Any())
            {
                Log.Error(
                    $"The following TrackIds were not found in table {tableName}: {string.Join(", ", notFoundTrackIds)}");
            }
        }

        private static async Task<Dictionary<Guid, demofrs>> GetUniqueEventsToBeInserted(List<demofrs> events)
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
                    OutputFields = { EventProcessor.EventCollectionProperties.TrackId, EventProcessor.EventCollectionProperties.EventId, },
                    ConsistencyLevel = ConsistencyLevel.Strong,
                    Offset = 0,
                    ExtraParameters = { ["nprobe"] = "128" },
                };

                var searchResults = await _uniquePeopleMilvusCollection.SearchAsync(
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

        public static async Task<Dictionary<Guid, demofrs>> GetEventsToBeInserted(List<demofrs> events)
        {
            try
            {
                if (!events.Any())
                    return new Dictionary<Guid, demofrs>();

            

                var toInsert = new Dictionary<Guid, demofrs>();
                for (int i = 0; i < events.Count; i++)
                {
              
                        toInsert.TryAdd(events[i].TrackId, events[i]);
                }

                return toInsert;
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
                
                var x = await _eventMilvusCollection.UpsertAsync(new FieldData[]
                {
                    FieldData.Create($"{EventProcessor.EventCollectionProperties.TrackId}",
                        events.Select(x => x.TrackId.ToString()).ToList()),
                    FieldData.Create($"{EventProcessor.EventCollectionProperties.EventId}",
                        events.Select(x => x.Id.ToString()).ToList()),
                    FieldData.CreateFloatVector($"{EventProcessor.EventCollectionProperties.Embedding}", embeddings),
                    FieldData.Create($"{EventProcessor.EventCollectionProperties.EventTime}",
                        events.Select(x => x.Time).ToList()),
                });
            
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }
        private static string BuildCleanLogsQuery(long time)
        {
            string schemaName = "events";
            string tableName = "Face_Recognition";
            string formattedTime = time.ToString();

            return $@"
WITH base_events AS (
    SELECT * 
    FROM {schemaName}.""{tableName}""
    WHERE ""isBaseImage"" = true AND ""Time"" <= '{formattedTime}'
),
candidate_events AS (
    SELECT be.""Id"" AS old_id, ce.""Id"" AS new_id
    FROM base_events be
    JOIN LATERAL (
        SELECT * 
        FROM {schemaName}.""{tableName}""
        WHERE ""GroupId"" = be.""GroupId""
          AND ""Time"" > be.""Time""
          AND ""faceWeight"" > 0.9
          AND ""detConf"" > 0.9
        ORDER BY ""Time"" ASC
        LIMIT 1
    ) ce ON true
),
update_new_base AS (
    UPDATE {schemaName}.""{tableName}"" ev
    SET ""isBaseImage"" = true
    FROM candidate_events ce
    WHERE ev.""Id"" = ce.new_id
    RETURNING ev.""Id"", ev.""TrackId"", ev.""embedding""
),
update_old_base AS (
    UPDATE {schemaName}.""{tableName}"" ev
    SET ""isBaseImage"" = false
    FROM candidate_events ce
    WHERE ev.""Id"" = ce.old_id
),
deleted_events AS (
    DELETE FROM {schemaName}.""{tableName}""
    WHERE ""Time"" <= '{formattedTime}'
    RETURNING ""Id""
)
SELECT 
    unb.""Id"" AS Id,
    unb.""TrackId"" AS TrackId,
    unb.""embedding""::real[] AS embedding,
    (SELECT MAX(""Id""::text) FROM deleted_events) AS ""MaxEventId""
FROM update_new_base unb;
";
        }

                public static void cleanLogs(long time)
                {
                    NpgsqlConnection npgsqlConnection = new NpgsqlConnection(DbConnectionString);
                    npgsqlConnection.Open();

                    
                    string query = BuildCleanLogsQuery(time);
        
                    var results = npgsqlConnection.Query<CleanlogsRow>(query);
        
                    if (results != null && results.Any())
                    {
                        var cleanLogs = new Cleanlogs
                        {
                            ReplaceEvents = results.Select(r => new demofrs
                            {
                                Id = r.Id,
                                TrackId = r.TrackId,
                                embedding = r.embedding
                            }).ToList(),
                            MaxEventId = results.FirstOrDefault()?.MaxEventId .ToString()?? string.Empty
                        };
                        RemoveEventsFromVectorDb(maxEventId: cleanLogs.MaxEventId).Wait();
                        AddEventsToVectorDb(cleanLogs.ReplaceEvents,npgsqlConnection);
        
                       
                    }
                }
        
        
        private static async Task AddEventsToVectorDb(List<demofrs> deletedEvents,
            NpgsqlConnection npgsqlConnection)
        {
            var dy = await GetEventsToBeInserted(deletedEvents);
            AddEventsToUniquePeopleQueueAsyc(dy,npgsqlConnection);
        }
        public static async Task deleteAllEventsByMaxId(string maxEventId)
        {
            try
            {
                if (!await IsVectorConnectedAsync(_uniquePeopleMilvusClient))
                {
                    throw new Exception("Vector is not connected");
                }

                var expression = $"{EventProcessor.EventCollectionProperties.EventId} <= '{maxEventId}'";
                _ = await _eventMilvusCollection.DeleteAsync(expression);
                _ = await _uniquePeopleMilvusCollection.DeleteAsync(expression);
            }
            catch (Exception ex)
            {
                Log.Error("error in [deleteAllEventsByMaxId] of [UniquepeopleReportManager] " + ex.Message);
                throw;
            }
        }
        private static async Task RemoveEventsFromVectorDb(string maxEventId)
        {
            try
            {
                await deleteAllEventsByMaxId(maxEventId);
            }
            catch (Exception ex)
            {
                Log.Error($"error in removing events from vector db: {ex.Message}");
            }
        }


    }
    
}
public class Cleanlogs
{
    public List<demofrs> ReplaceEvents { get; set; }
    public string MaxEventId { get; set; }

    public Cleanlogs()
    {
        ReplaceEvents = new List<demofrs>();
        MaxEventId = string.Empty;
    }
}
// Helper class to match the query result structure
public class CleanlogsRow
{
    public Guid Id { get; set; }
    public Guid TrackId { get; set; }
    public float[] embedding { get; set; }
    public string MaxEventId { get; set; }
}