using System.Diagnostics;
using System.Text;
using Dapper;
using MassTransit;
using Milvus.Client;
using Npgsql;
using Pgvector;
using Serilog;

namespace MyApp;

public static class EventProcessor
{

    public static List<ReadOnlyMemory<float>> embeddingDataReceived = new List<ReadOnlyMemory<float>>();
  
 public static string Id = Guid.Empty.ToString();
    public static List<long> eventTimes = new List<long>();
    public static List<string> trackIds = new List<string>();
    public static List<string> eventIds = new List<string>();
    public static double FaceMatchThresholdValue = 0.5;
    public static string demoReportLastId = "demoReportLastId";

    private static string updateWithNewGroupIdsBaseQuery =
        $"UPDATE events.\"Face_Recognition\" set \"GroupId\" = temp_updates.groupId::uuid FROM temp_updates WHERE events.\"Face_Recognition\".\"Id\"::uuid = temp_updates.id::uuid;\n";

    private static string updateWithExistingGroupIdsBaseQuery =
        $"UPDATE events.\"Face_Recognition\" set \"GroupId\" = (select f.\"GroupId\" from events.\"Face_Recognition\" as f where f.\"Id\" = temp_updates.groupId::UUID limit 1) FROM temp_updates WHERE events.\"Face_Recognition\".\"Id\"::uuid = temp_updates.id::uuid;\n";

    private static NpgsqlConnection npgsqlConnection;

    public static class EventCollectionProperties
    {
        public const string TrackId = "track_id";
        public const string EventId = "event_id";
        public const string Embedding = "embedding";
        public static string VideoSourceId = "device_id";
        public const string EventTime = "event_time";
    }

    public static async Task StartGroupIdAssignWork(string connectionString)
    {
        NpgsqlConnection npgsqlConnection = new NpgsqlConnection(
            connectionString
        );
        npgsqlConnection.Open();
        
        string selectQuery = @"SELECT ""Value"" FROM public.""SystemConfig"" WHERE ""Key"" = @key;";
        string demoReportLastIdKey = "demoReportLastId";

        var value = npgsqlConnection.QueryFirstOrDefault<string>(selectQuery, new { key = demoReportLastIdKey });
        if (value == null)
        {
            var newid = Guid.NewGuid();
            string insertQuery = @"INSERT INTO public.""SystemConfig"" (""Id"",""Key"", ""Value"") VALUES (@Id,@key, @value);";
             npgsqlConnection.QueryFirstOrDefault<string>(insertQuery, new {Id = newid, key = demoReportLastIdKey , value = Id });
        }
        if (!string.IsNullOrEmpty(value))
        {
            Id = value; 
        }

        await assignGroupIdsToEvents(npgsqlConnection);
        npgsqlConnection.Close();
    }

    private static async Task assignGroupIdsToEvents(NpgsqlConnection npgsqlConnection)
    {try
        {
            var currentTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            var maxRec = currentTime - 7200000;
            var processedEvent = 0;
            var limit = 500;
            var offset = 0;
            
            while (true)
            {
                Console.WriteLine("groupidassigning Log");
                Stopwatch stopwatch = Stopwatch.StartNew();
                var getFaceEventsFromDbQuery =
                    $"select e.\"Id\",e.\"embedding\" ::real[], e.\"TrackId\", e.\"ReceivedTime\" from events.\"Face_Recognition\" as e where e.\"Id\">'{Id}' and e.\"ReceivedTime\" < {maxRec} ORDER BY e.\"Id\" FETCH NEXT ({limit}) ROWS ONLY;";
                //Console.WriteLine(getFaceEventsFromDbQuery);
                var events = npgsqlConnection
                    .Query<MilvusEventSchemaParameters>(getFaceEventsFromDbQuery)
                    .ToList();
                if (events.Count == 0)
                {
                    break;
                }
                foreach (var milvusEventSchemaParameterse in events)
                {
                    if (milvusEventSchemaParameterse.embedding == null ||
                        milvusEventSchemaParameterse.embedding.Length == 0)
                    {
                        Console.WriteLine("No embedding found");
                    }

                    if (milvusEventSchemaParameterse.embedding is not float[] ||
                        milvusEventSchemaParameterse.embedding.Length < 512)
                    {
                        Console.WriteLine("Embedding not found");
                    }
                    Console.WriteLine(milvusEventSchemaParameterse.embedding[0]);
                }

                // var embeddings = eventsToBeInsertedList.Select(e => new ReadOnlyMemory<float>(((Vector)e.EventProperties["embedding"]).ToArray())).ToList();
                List<ReadOnlyMemory<float>> embeddings = events
                    .Select(e => new ReadOnlyMemory<float>(e.embedding.ToArray()))
                    .ToList();
                var eventGroupInfos = await CheckForEventGroupIdInBatch(
                    embeddings, events.First().Id
                );
                var updateWithNewGuidQueries = new StringBuilder();
                var updateWithNewGuidqueryList = new List<string>();
                updateWithNewGuidQueries.Append("with temp_updates (id, groupId) As ( Values ");
                var updateWithExistingGuidQueries = new StringBuilder();
                var updateWithExistingGuidqueryList = new List<string>();
                updateWithExistingGuidQueries.Append("with temp_updates (id, groupId) As ( Values ");

                for (int i = 0; i < eventGroupInfos.Count; i++)
                {
                 
                    var eventGroupInfo = eventGroupInfos[i];
                    if (eventGroupInfo.GroupId != null)
                    {
                        updateWithNewGuidqueryList.Add($"(\'{events[i].Id}\', \'{eventGroupInfo.GroupId}\')");
                    }
                    else
                    {
                        updateWithExistingGuidqueryList.Add($"(\'{events[i].Id}\', \'{eventGroupInfo.EventId}\')");
                    }
                }

                updateWithNewGuidQueries.Append(string.Join(',', updateWithNewGuidqueryList));
                updateWithNewGuidQueries.Append(")\n");
                updateWithNewGuidQueries.Append(updateWithNewGroupIdsBaseQuery);
                updateWithExistingGuidQueries.Append(string.Join(',', updateWithExistingGuidqueryList));
                updateWithExistingGuidQueries.Append(")\n");
                updateWithExistingGuidQueries.Append(updateWithExistingGroupIdsBaseQuery);

                var batchQueries = new StringBuilder();
                Id = events.Last().Id.ToString();
                if (updateWithNewGuidqueryList.Count > 0)
                {
                    batchQueries.Append(updateWithNewGuidQueries); // No ToString() needed
                }

                if (updateWithExistingGuidqueryList.Count > 0)
                {
                    batchQueries.Append(updateWithExistingGuidQueries); // No ToString() needed
                }

                batchQueries.Append(
                    $"UPDATE public.\"SystemConfig\" SET \"Value\"= '{Id}' WHERE \"Key\" = '{demoReportLastId}';");
                //Log.Error(batchQueries.ToString());
                npgsqlConnection.Query(batchQueries.ToString());
                offset = offset + limit;
                processedEvent += events.Count;
                stopwatch.Stop();
                var logstring = "---------------- ProcessEventsNotHavingGroupIds in EventProcessor Done : " + processedEvent + " Total time taken: " + stopwatch.Elapsed;
                Console.WriteLine(logstring);
                File.AppendAllTextAsync(Program.timelogTxt, logstring);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("error in [UpdateGuidInEvents] of [UniquepeopleReportManager] " + ex.Message);
        }
    }

    private static void ProcessEmptyResults(IReadOnlyList<ReadOnlyMemory<float>> embeddings)
    {
        for (int i = 0; i < embeddings.Count; i++)
        {
            MaintainCluster(embeddings[i]);
        }
    }
    public static Dictionary<string, List<ReadOnlyMemory<float>>> clusterInfo =
        new Dictionary<string, List<ReadOnlyMemory<float>>>();

    public static List<(string EventId, string TrackId,  string GroupId
        )> groupInfos = new();


   public static async Task<List<(string EventId, string TrackId, string GroupId)>>
        CheckForEventGroupIdInBatch(IReadOnlyList<ReadOnlyMemory<float>> embeddings, Guid eventId)
    {
        eventTimes.Clear();
        trackIds.Clear();
        clusterInfo.Clear();
        groupInfos.Clear();
        eventIds.Clear();
        try
        {
            var parameters = new SearchParameters
            {
                OutputFields =
                {
                  EventCollectionProperties.TrackId,
                    EventCollectionProperties.EventId,
                    
                },
                ConsistencyLevel = ConsistencyLevel.Strong,
                Offset = 0,
                Expression = $"{EventCollectionProperties.EventId} < '{eventId}'",
                ExtraParameters = { ["nprobe"] = "128" },
            };
           
            var searchResult = await Program._uniquePeopleMilvusCollection.SearchAsync(EventCollectionProperties.Embedding,
                embeddings,
                SimilarityMetricType.Ip, limit: 1, parameters);


            if (searchResult is null || searchResult.Scores.Count < 1)
            {
                // Handle empty results more explicitly
                ProcessEmptyResults(embeddings);
            }
            else
            {
                var resultGroupIds = searchResult.FieldsData;

                var trackIdsFieldData = resultGroupIds
                    .Where(x => x.FieldName == $"{EventCollectionProperties.TrackId}").FirstOrDefault();

                var eventIdsFieldData = resultGroupIds
                    .Where(x => x.FieldName == $"{EventCollectionProperties.EventId}").FirstOrDefault();
                

                if (trackIdsFieldData is FieldData<string> trackIdFields)
                {
                    trackIds = trackIdFields.Data.ToList();
                }

                if (eventIdsFieldData is FieldData<string> eventIdFields)
                {
                    eventIds = eventIdFields.Data.ToList();
                }


             


                ProcessSearchResults(embeddings, searchResult.Scores);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Exception occuredd" + ex.Message);
            ProcessEmptyResults(embeddings);
        }

        return groupInfos;
    }


    private static void ProcessSearchResults(IReadOnlyList<ReadOnlyMemory<float>> embeddings,
        IReadOnlyList<float> scores)
    {
        // Use a faster for loop with direct indexing
        for (int i = 0; i < embeddings.Count; i++)
        {
            // This check now just logs but doesn't block execution since we're only checking base images


            if (scores[i] < FaceMatchThresholdValue)
            {
                MaintainCluster(embeddings[i]);
            }
            else
            {
                // Pre-construct the tuple to avoid multiple allocations
                //var indexstatus =  _milvusCollection.DescribeIndexAsync("embedding", "embedding").Result;

                groupInfos.Add((
                    EventId: eventIds[i],
                    TrackId: trackIds[i],
                    GroupId: null
                ));
            }
        }
    }


    private static void MaintainCluster(ReadOnlyMemory<float> embedding)
    {
        string groupId = null;
        double currentRecConf = 0.0;
        foreach (var clusters in clusterInfo)
        {
            foreach (var cluster in clusters.Value)
            {
                var cosineSimilarity = InnerProduct(cluster, embedding);
                if (cosineSimilarity > 0.5 && cosineSimilarity > currentRecConf)
                {
                    currentRecConf = cosineSimilarity;
                    groupId = clusters.Key;
                }
            }
        }

        if (groupId == null)
        {
            groupId = Guid.NewGuid().ToString();
            clusterInfo.Add(groupId, new List<ReadOnlyMemory<float>>());
            
        }
        
        clusterInfo[groupId].Add(embedding);
        groupInfos.Add((EventId: null, TrackId: null, 
            GroupId: groupId));
    }
    public static double InnerProduct(ReadOnlyMemory<float> vectorA, ReadOnlyMemory<float> vectorB)
    {
        var spanA = vectorA.Span;
        var spanB = vectorB.Span;
        double sum = 0;
        for (int i = 0; i < spanA.Length; i++)
        {
            sum += spanA[i] * spanB[i];
        }

        return sum;
    }
    
}
public class MilvusEventSchemaParameters
{
    public Guid Id { get; set; }
    public Guid TrackId { get; set; }
    public long ReceivedTime { get; set; }
    public float[] embedding { get; set; }
}