from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from typing import Dict, Any
import os


class SCDHandler:
    def __init__(self, spark_session):
        self.spark = spark_session

    def process(self, df: DataFrame, output_config: Dict[str, Any]):
        store_type = output_config.get('store_type', 'SCD1').upper()
        if store_type == 'SCD1':
            self.handle_scd1(df, output_config)
        elif store_type == 'SCD2':
            self.handle_scd2(df, output_config)
        elif store_type == 'SCD3':
            self.handle_scd3(df, output_config)
        elif store_type == 'SCD4':
            self.handle_scd4(df, output_config)
        else:
            raise ValueError(f"Unsupported SCD type: {store_type}")

    def handle_scd1(self, df: DataFrame, output_config: Dict[str, Any]):
        """
        Implements SCD Type 1 by overwriting the existing data with new data.
        """
        output_path = output_config['path']
        primary_key = output_config['primary_key']
        partition_by = output_config.get('partition_by', [])
        data_format = output_config.get('format', 'parquet')
        mode = output_config.get('mode', 'overwrite')

        if DeltaTable.isDeltaTable(self.spark, output_path):
            delta_table = DeltaTable.forPath(self.spark, output_path)
        else:
            df.write.format('delta').partitionBy(
                partition_by).mode('overwrite').save(output_path)
            return

        merge_condition = ' AND '.join(
            [f"tgt.{col} =src.{col}" for col in primary_key])

        delta_table.alias("tgt").merge(
            source=df.alias("src"),
            condition=merge_condition
        ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    def handle_scd2(self, df: DataFrame, output_config: Dict[str, Any]):
        """
        Implements SCD Type 2 by keeping historical records with start and end dates.
        """
        output_path = output_config['path']
        primary_key = output_config['primary_key']
        tracking_columns = output_config.get('tracking_columns', [])
        data_format = output_config.get('format', 'delta')

        if data_format != 'delta':
            raise ValueError("SCD2 requires 'delta' format.")

        if DeltaTable.isDeltaTable(self.spark, output_path):
            delta_table = DeltaTable.forPath(self.spark, output_path)
        else:
            df.limit(0).write.format('delta').save(output_path)
            delta_table = DeltaTable.forPath(self.spark, output_path)

        df_updates = df.withColumn('start_date', F.current_timestamp()) \
            .withColumn('end_date', F.lit(None).cast('timestamp')) \
            .withColumn('is_current', F.lit(True))

        merge_condition = ' AND '.join(
            [f"tgt.{col} = src.{col}" for col in primary_key])

        update_condition = ' OR '.join(
            [f"src.{col} <> tgt.{col}" for col in tracking_columns])

        delta_table.alias('tgt').merge(
            df_updates.alias('src'),
            merge_condition
        ).whenMatchedUpdate(
            condition=f"tgt.is_current = True AND ({update_condition})",
            set={
                'end_date': F.current_timestamp(),
                'is_current': F.lit(False)
            }
        ).whenNotMatchedInsert(
            values={
                **{col: F.col(f"src.{col}") for col in df_updates.columns}
            }
        ).execute()

    def handle_scd3(self, df: DataFrame, output_config: Dict[str, Any]):
        """
        Implements SCD Type 3 by adding new columns to store previous values.
        """
        output_path = output_config['path']
        primary_key = output_config['primary_key']
        tracking_columns = output_config.get('tracking_columns', [])
        partition_by = output_config.get('partition_by', [])
        data_format = output_config.get('format', 'delta')

        if DeltaTable.isDeltaTable(self.spark, output_path):
            delta_table = DeltaTable.forPath(self.spark, output_path)
            df_existing = delta_table.toDF()
        else:
            df_existing = self.spark.createDataFrame([], df.schema)

        join_condition = ' AND ' \
            .join([f"src.{col} = tgt.{col}" for col in primary_key])
        df_joined = df.alias('src') \
            .join(df_existing.alias('tgt'), on=primary_key, how='left')

        for col_name in tracking_columns:
            df_joined = df_joined.withColumn(
                f"prev_{col_name}", F.coalesce(
                    F.col(f'tgt.{col_name}'), F.lit(None))
            ).withColumn(
                col_name, F.col(f"src.{col_name}")
            )

        selected_columns = df.columns + \
            [f"prev_{col}" for col in tracking_columns]
        df_result = df_joined.select(*selected_columns)

        df_result.write.format(data_format) \
            .mode("overwrite") \
            .partitionBy(partition_by) \
            .save(output_path)

    def handle_scd4(self, df: DataFrame, output_config: Dict[str, Any]):
        """
        Implements SCD Type 4 by maintaining a current dimension table and a separate history table.
        """
        current_table_path = output_config['path']
        history_table_path = output_config.get('history_path')
        primary_key = output_config['primary_key']
        partition_by = output_config.get('partition_by', [])
        data_format = output_config.get('format', 'delta')

        if not history_table_path:
            raise ValueError(
                "For SCD Type 4, 'history_path' must be specified in the output configuration."
            )

        self.spark.conf.set(
            "spark.databricks.delta.schema.autoMerge.enabled", "true")

        print("Checking if current_table_path is a Delta table...")
        if DeltaTable.isDeltaTable(self.spark, current_table_path):
            print(
                f"{current_table_path} is a Delta table. Merging data...")
            current_table = DeltaTable.forPath(self.spark, current_table_path)
            merge_condition = ' AND '.join(
                [f"tgt.{col}=src.{col}" for col in primary_key])

            try:
                print(
                    f"Merge condition for current table: {merge_condition}")
                current_table.alias("tgt").merge(
                    source=df.alias("src"),
                    condition=merge_condition
                ).whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
                print("Merge into current table completed successfully.")
            except Exception as e:
                printr(
                    f"Error merging data into current table {current_table_path}: {str(e)}")
                raise
        else:
            print(
                f"{current_table_path} is not a Delta table. Overwriting with new data...")
            df.write.format("delta") \
                .mode("overwrite") \
                .partitionBy(partition_by) \
                .save(current_table_path)
            print(
                f"Data written to current table at {current_table_path}")

        # Process history table
        df_history = df.withColumn("record_timestamp", F.current_timestamp())
        print("Preparing to write history data...")
        print(
            f"Checking if history_table_path {history_table_path} is a Delta table.")

        if DeltaTable.isDeltaTable(self.spark, history_table_path):
            print(
                f"{history_table_path} is a Delta table. Loading existing schema...")
            try:
                existing_history_df = self.spark.read.format(
                    'delta').load(history_table_path)
                print(
                    f"Loaded existing history table schema: {existing_history_df.schema}")
            except Exception as e:
                printr(
                    f"Error reading existing history table at {history_table_path}: {str(e)}")
                raise

            if df_history.schema != existing_history_df.schema:
                print(
                    "Schema evolution detected. Merging schema before appending.")
                print(f"New DataFrame schema: {df_history.schema}")
                print(
                    f"Existing history schema: {existing_history_df.schema}")
                try:
                    df_history.write.format("delta") \
                        .mode("append") \
                        .option("mergeSchema", "true") \
                        .partitionBy(partition_by) \
                        .save(history_table_path)
                    print(
                        f"Appended new history data to {history_table_path} with schema merge.")
                except Exception as e:
                    printr(
                        f"Error appending with schema merge to {history_table_path}: {str(e)}")
                    raise
            else:
                print(
                    "No schema evolution detected. Appending to history directly.")
                try:
                    df_history.write.format("delta") \
                        .mode("append") \
                        .partitionBy(partition_by) \
                        .save(history_table_path)
                    print(
                        f"Appended new history data to {history_table_path}.")
                except Exception as e:
                    printr(
                        f"Error appending to {history_table_path}: {str(e)}")
                    raise
        else:
            print(
                f"{history_table_path} is not a Delta table. Creating new history table...")
            try:
                df_history.write.format("delta") \
                    .mode("overwrite") \
                    .partitionBy(partition_by) \
                    .save(history_table_path)
                print(f"History table created at {history_table_path}")
            except Exception as e:
                printr(
                    f"Error creating history table at {history_table_path}: {str(e)}")
                raise

        print("SCD4 processing completed successfully.")
