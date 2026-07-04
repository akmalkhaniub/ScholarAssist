from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def resolve_clusters(edges_df: DataFrame) -> DataFrame:
    """
    Takes duplicate pairs from blocking and LSH.
    Uses GraphFrames Connected Components to group records into clusters,
    assigning a single golden_record_id to each cluster.
    """
    try:
        from graphframes import GraphFrame
    except ImportError:
        raise ImportError("GraphFrames is required for connected components resolution. Please include the package.")

    v1 = edges_df.select(F.col("id1").alias("id"))
    v2 = edges_df.select(F.col("id2").alias("id"))
    vertices = v1.union(v2).distinct()
    
    edges = edges_df.select(
        F.col("id1").alias("src"), 
        F.col("id2").alias("dst")
    )
    
    g = GraphFrame(vertices, edges)
    
    # Checkpoint dir is required by connectedComponents
    spark = edges_df.sparkSession
    spark.sparkContext.setCheckpointDir("/tmp/graphframes_checkpoints")
    
    cc = g.connectedComponents()
    
    clusters = cc.select(
        F.col("id"), 
        F.col("component").cast("string").alias("golden_record_id")
    )
    
    return clusters
