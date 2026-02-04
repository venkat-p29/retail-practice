import pyspark.sql.functions as F

def validate_dates_count(df, date_col: str) -> None:
    return df.filter(F.col(date_col).isNull()).count()


def write_to_table(spark, df, target: str, env: str, merge_query: str) -> str:
    """
    Writes a Spark DataFrame to a Delta table with logic based on the environment.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    df : pyspark.sql.DataFrame
        DataFrame to write.
    target : str
        Fully qualified Delta table name.
    env : str
        Environment name ('dev', 'stage', 'prod').
    merge_query : str
        SQL MERGE statement for upsert logic.

    Returns
    -------
    str
        Summary of the write operation.

    Behavior
    --------
    - In 'dev', overwrites the target table with the DataFrame and schema.
    - In 'stage' or 'prod':
        - If the table does not exist, creates it and loads the data.
        - If the table exists, executes the provided MERGE query to upsert records.
    """
    output = f"Environment: {env}\n\n"

    if env == "dev":
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        output += f"Table {target} is overwritten."
    else:
        if not spark.catalog.tableExists(target):
            df.write.saveAsTable(target)
            output += f"Table {target} is created and data is loaded."
        else:
            output += "Executing Delta Merge query...\n"
            spark.sql(merge_query)
            output += f"Table {target} is updated."
    
    return output
