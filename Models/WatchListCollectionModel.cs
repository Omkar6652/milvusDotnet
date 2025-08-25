using Pgvector;

namespace MyApp.Models;

public class WatchListCollectionModel
{
    public Guid FaceId { get; set; }
    public Guid PersonId { get; set; }
    public float[] Embeddings { get; set; }
}