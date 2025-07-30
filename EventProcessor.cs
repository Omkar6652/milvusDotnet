using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Qdrant.Client;
using Qdrant.Client.Grpc;
using Dapper;
using Google.Protobuf.Collections;
using Npgsql;
using Serilog;
using Range = Qdrant.Client.Grpc.Range;

namespace MyApp;

public static class EventProcessor
{
    public static string Id = Guid.Empty.ToString();
    public static double FaceMatchThresholdValue = 0.5;
    public static string demoReportLastId = "demoReportLastId";
    private static string updateWithNewGroupIdsBaseQuery =
        $"UPDATE events.\"Face_Recognition\" set \"GroupId\" = temp_updates.groupId::uuid FROM temp_updates WHERE events.\"Face_Recognition\".\"Id\"::uuid = temp_updates.id::uuid;\n";

    private static string updateWithExistingGroupIdsBaseQuery =
        $"UPDATE events.\"Face_Recognition\" set \"GroupId\" = (select f.\"GroupId\" from events.\"Face_Recognition\" as f where f.\"Id\" = temp_updates.groupId::UUID limit 1) FROM temp_updates WHERE events.\"Face_Recognition\".\"Id\"::uuid = temp_updates.id::uuid;\n";

    public static async Task StartGroupIdWork(string connectionString)
    {
        using var conn = new NpgsqlConnection(connectionString);
        conn.Open();

        string selectQuery = @"SELECT ""Value"" FROM public.""SystemConfig"" WHERE ""Key"" = @key;";
        var value = conn.QueryFirstOrDefault<string>(selectQuery, new { key = demoReportLastId });

        if (string.IsNullOrEmpty(value))
        {
            var newId = Guid.NewGuid();
            string insertQuery =
                @"INSERT INTO public.""SystemConfig"" (""Id"", ""Key"", ""Value"") VALUES (@Id, @key, @value);";
            conn.Execute(insertQuery, new { Id = newId, key = demoReportLastId, value = Id });
        }
        else
        {
            Id = value;
        }

        await assignGroupIdsToEvents(conn);
        conn.Close();
    }

    private static async Task assignGroupIdsToEvents(NpgsqlConnection conn)
    {
        var currentTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        var maxRec = currentTime - 7200000; // 2 hours ago
        var processed = 0;
        var limit = 500;

        while (true)
        {
            var query = $@"
                    SELECT ""Id"", ""embedding""::real[], ""TrackId"", ""Time""
                    FROM events.""Face_Recognition""
                    WHERE ""Id"" > '{Id}' AND ""Time"" < {maxRec}
                    ORDER BY ""Id"" ASC
                    LIMIT {limit};";

            var events = conn.Query<demofrs>(query).ToList();
            if (events.Count == 0) break;

            var embeddings = events.Select(e => e.embedding).ToList();
            var groupInfos = await CheckForEventGroupIdInBatch(embeddings, events.First().Time);

             var updateWithNewGuidQueries = new StringBuilder();
                var updateWithNewGuidqueryList = new List<string>();
                updateWithNewGuidQueries.Append("with temp_updates (id, groupId) As ( Values ");
                var updateWithExistingGuidQueries = new StringBuilder();
                var updateWithExistingGuidqueryList = new List<string>();
                updateWithExistingGuidQueries.Append("with temp_updates (id, groupId) As ( Values ");

                for (int i = 0; i < groupInfos.Count; i++)
                {
                 
                    var eventGroupInfo = groupInfos[i];
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
            _ =    conn.Query(batchQueries.ToString());
                processed += events.Count;
        }
    }

    public static List<(string EventId, string TrackId, string GroupId)> groupInfos = new();
    public static Dictionary<string, List<float[]>> clusterInfo = new();

    public static async Task<List<(string EventId, string TrackId, string GroupId)>> CheckForEventGroupIdInBatch(
        List<float[]> embeddings, long eventTime)
    {
        groupInfos.Clear();
        clusterInfo.Clear();

        var searchPointsList = new List<SearchPoints>();

        foreach (var embedding in embeddings)
        {
            // Ensure embedding is a float array for RepeatedField
            float[] vectorArray = embedding.ToArray(); // Or handle ReadOnlyMemory.Span if preferred

            var searchRequest = new SearchPoints
            {
                CollectionName = Program.EventCollectionName, // Set collection name for each request
                Vector = { vectorArray }, // RepeatedField<float> accepts IEnumerable<float>
                Limit = 1,
                WithPayload = true, // Assuming you need payload data back
                // WithVectors = false, // Add if you need vector data back
                Params = new SearchParams { HnswEf = (ulong)Program.Ef }, // Set HNSW ef parameter
                // Create the Filter object
                Filter = new Filter
                {
                    // Add conditions to the 'Must' list for AND logic
                    Must =
                    {
                        new Condition
                        {
                            Field = new FieldCondition
                            {
                                Key = "time", // Use the correct payload key (no "payload." prefix in Qdrant gRPC filters)
                                Range = new Range { Lt = (double)eventTime } // Lt for Less Than (use double)
                            }
                        }
                        // Add more conditions to 'Must' if needed
                    }
                    // Use 'Should' for OR logic, 'MustNot' for NOT logic if needed
                }
                // Offset, ScoreThreshold, etc. can be set here if required
            };

            searchPointsList.Add(searchRequest);
        }

        Stopwatch sw = Stopwatch.StartNew();
        // 2. Execute the batch search using the QdrantClient instance method
        // This is the correct signature for Qdrant.Client v1.12.0
        var batchResults = await Program._qdrantClient.SearchBatchAsync(
            collectionName: Program
                .EventCollectionName, // Collection name (often set per request, but method requires it)
            searches: searchPointsList, // The list of SearchPoints requests
            readConsistency: null, // Optional: ReadConsistency
            timeout: null, // Optional: Timeout
            cancellationToken: default // Optional: Cancellation Token
        );


        sw.Stop();
        Console.WriteLine(sw.Elapsed + ": Elapsed while searching qdrant in batch");

        for (int i = 0; i < batchResults.Count; i++)
        {
            var batchResult = batchResults[i]; // This is a BatchResult for embeddings[i]
            var emb = embeddings[i]; // The original embedding

            // Check if any results were returned for this specific embedding's search
            // and if the best score meets or exceeds the threshold.
            if (batchResult.Result.Count == 0 || batchResult.Result[0].Score < FaceMatchThresholdValue)
            {
                // Case 1: No results found, or
                // Case 2: Best result's score is below the threshold.
                // In either case, consider it a new, unique item.
                MaintainCluster(emb);
            }
            else
            {
                // Case 3: A result was found with a score >= threshold.
                // Use the GroupId/EventId from the matched point.
                var topMatchedPoint = batchResult.Result[0]; // The best match
                var payload = topMatchedPoint.Payload; // Its payload dictionary

                // Safely extract payload values
                string eventId = "N/A"; // Default if not found
                string trackId = "N/A"; // Default if not found

                if (payload != null) // Check if payload exists
                {
                    // Use TryGetValue for safety
                    if (payload.TryGetValue("event_id", out var eventIdValue))
                    {
                        eventId = eventIdValue.StringValue ?? "N/A"; // Assuming it's stored as a string
                    }

                    if (payload.TryGetValue("track_id", out var trackIdValue))
                    {
                        trackId = trackIdValue.StringValue ?? "N/A"; // Assuming it's stored as a string
                    }
                }
                else
                {
                    Console.WriteLine($"payload was null");
                }

                // Add to groupInfos, indicating it should use the GroupId from eventId
                // (You might need to adjust the logic here based on whether eventId or GroupId is the key you need)
                groupInfos.Add((
                    EventId: eventId, // ID of the matched point
                    TrackId: trackId, // TrackId of the matched point
                    GroupId: null // Signal that the GroupId should be copied from EventId's record
                ));
            }
        }

        return groupInfos;
    }


    private static void MaintainCluster(float[] embedding)
    {
        string bestGroupId = null;
        double maxSim = 0.0;

        foreach (var cluster in clusterInfo)
        {
            foreach (var vec in cluster.Value)
            {
                var sim = InnerProduct(embedding, vec);
                if (sim > 0.5 && sim > maxSim)
                {
                    maxSim = sim;
                    bestGroupId = cluster.Key;
                }
            }
        }

        if (bestGroupId == null)
        {
            bestGroupId = Guid.NewGuid().ToString();
            clusterInfo[bestGroupId] = new List<float[]>();
        }

        clusterInfo[bestGroupId].Add(embedding);
        groupInfos.Add((EventId: null, TrackId: null, GroupId: bestGroupId));
    }

    public static double InnerProduct(float[] a, float[] b)
    {
        double sum = 0;
        for (int i = 0; i < a.Length; i++)
            sum += a[i] * b[i];
        return sum;
    }
}