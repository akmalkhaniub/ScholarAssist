from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import MapType, StringType

def merge_golden_records(df: DataFrame, cluster_mapping: DataFrame, provider_priority: list[str]) -> DataFrame:
    """
    Aggregates the records in each cluster into a single Golden Record.
    Resolves conflicts using provider_priority (e.g. Crossref > OpenAlex > Semantic Scholar).
    Generates field-level source provenance tracking (source_provenance map).
    """
    df_clustered = df.join(cluster_mapping, on="id", how="left")
    
    df_clustered = df_clustered.withColumn(
        "golden_record_id",
        F.coalesce(F.col("golden_record_id"), F.col("id").cast("string"))
    )
    
    # Assign priority rank based on provider_priority array
    priority_expr = F.lit(len(provider_priority))
    for i, provider in enumerate(provider_priority):
        priority_expr = F.when(F.col("provider") == provider, i).otherwise(priority_expr)
        
    df_clustered = df_clustered.withColumn("priority_rank", priority_expr)
    
    # List of fields to resolve
    fields_to_resolve = ["title", "doi", "publication_year", "first_author_lastname"]
    
    agg_exprs = []
    prov_exprs = []
    
    for field in fields_to_resolve:
        if field in df.columns:
            # Create a struct with priority rank, provider, and value
            struct_col = F.struct(
                F.col("priority_rank"), 
                F.col("provider"), 
                F.col(field).alias("value")
            )
            # Only consider structs where the value is not null
            valid_struct = F.when(F.col(field).isNotNull(), struct_col)
            best_struct = F.min(valid_struct)
            
            agg_exprs.append(best_struct.getField("value").alias(field))
            prov_exprs.append(F.lit(field))
            prov_exprs.append(best_struct.getField("provider"))

    agg_exprs.append(F.collect_list("id").alias("merged_ids"))
    agg_exprs.append(F.collect_set("provider").alias("merged_providers"))
    
    merged_df = df_clustered.groupBy("golden_record_id").agg(*agg_exprs)
    
    if prov_exprs:
        merged_df = merged_df.withColumn(
            "source_provenance",
            F.create_map(*prov_exprs)
        )
    else:
        merged_df = merged_df.withColumn(
            "source_provenance", 
            F.create_map().cast(MapType(StringType(), StringType()))
        )
        
    return merged_df
