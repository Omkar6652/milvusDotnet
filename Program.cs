using System;
using System.Collections.ObjectModel;
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
                        FieldSchema.Create<bool>("is_base_image"),
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
                // await _milvusCollection.LoadAsync();
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

                // Console.WriteLine("Hello World!");
                //  await ProcessEventsNotHavingGroupIds();
                // File.WriteAllText(eventIdFileName, Id);
                // await ProcessIndexWork();

                await EventProcessor.StartGroupIdWork(DbConnectionString);
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
                    var getFaceEventsFromDbQuery = "";

                    getFaceEventsFromDbQuery =
                        $"select e.\"Id\",e.\"embedding\"::real[],e.\"TrackId\", e.\"ReceivedTime\",\"detConf\", \"faceWeight\", \"VideoSourceId\"  from events.\"Face_Recognition\" as e where e.\"Id\">'{Id}'   ORDER BY e.\"Id\" FETCH NEXT ({limit}) ROWS ONLY;";


                    var events = npgsqlConnection
                        .Query<demofrs>(getFaceEventsFromDbQuery)
                        .ToList();
                    if (events.Count == 0)
                    {
                        break;
                    }

                    Dictionary<Guid, demofrs> eventsToBeinserted = new();
                    foreach (var item in events)
                    {
                        if (item.detConf > 0.9 && item.faceWeight > 0.9)
                        {
                            if (await isEventToBeInserted(item))
                            {
                                eventsToBeinserted[item.TrackId] = item;
                            }
                        }
                    }

                    await AddEventsToQueueAsync(eventsToBeinserted);
                    offset = offset + limit;
                    processedEvent += events.Count;
                    Id = events.Last().Id.ToString();
                    Console.WriteLine("ProcessEventsNotHavingGroupIds Done : " + processedEvent);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            npgsqlConnection.Close();
        }

        public static async Task<bool> isEventToBeInserted(demofrs demofrs)
        {
            var parameters = new SearchParameters
            {
                OutputFields =
                {
                    "track_id",
                    "event_id",
                    "event_time"
                },
                ConsistencyLevel = ConsistencyLevel.Strong,
                Offset = 0,
                Expression =
                    $"{EventProcessor.EventCollectionProperties.EventId} < '{demofrs.Id}' && {EventProcessor.EventCollectionProperties.isBaseImage} == true",
                ExtraParameters = { ["ef"] = "130" }
            };
            List<ReadOnlyMemory<float>> embeddings = new List<ReadOnlyMemory<float>>()
                { new ReadOnlyMemory<float>(demofrs.embedding.ToArray()) };


            var searchResult = await Program._milvusCollection.SearchAsync(
                EventProcessor.EventCollectionProperties.Embedding,
                embeddings,
                SimilarityMetricType.Ip, limit: 1, parameters);


            if (searchResult is null || searchResult.Scores.Count < 1)
            {
                return true;
            }

            return false;
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