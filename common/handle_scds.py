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
        partition_by = output_config.get('partition_by', [])
        data_format = output_config.get('format', 'parquet')
        mode = output_config.get('mode', 'overwrite')

        df.write.format(data_format) \
            .mode(mode) \
            .partitionBy(partition_by) \
            .save(output_path)

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
        ).whenNotMatchedInsertAll().execute()

    def handle_scd3(self, df: DataFrame, output_config: Dict[str, Any]):
        """
        Implements SCD Type 3 by adding new columns to store previous values.
        """
        output_path = output_config['path']
        primary_key = output_config['primary_key']
        tracking_columns = output_config.get('tracking_columns', [])
        partition_by = output_config.get('partition_by', [])
        data_format = output_config.get('format', 'parquet')
        mode = output_config.get('mode', 'overwrite')

        if os.path.exists(output_path):
            df_existing = self.spark.read.format(data_format).load(output_path)
        else:
            df_existing = self.spark.createDataFrame([], df.schema)

        join_condition = [df[col] == df_existing[col] for col in primary_key]
        df_joined = df.join(df_existing, on=join_condition, how='left')

        for col_name in tracking_columns:
            df_joined = df_joined.withColumn(f'prev_{col_name}',
                                             F.coalesce(df_existing[col_name], F.lit(None))) \
                .withColumn(col_name, df[col_name])

        selected_columns = df.columns + \
            [f'prev_{col}' for col in tracking_columns]
        df_result = df_joined.select(*selected_columns)

        df_result.write.format(data_format) \
            .mode(mode) \
            .partitionBy(partition_by) \
            .save(output_path)

    def handle_scd4(self, df: DataFrame, output_config: Dict[str, Any]):
        """
        Implements SCD Type 4 by maintaining a current dimension table and a separate history table.
        """
        current_table_path = output_config['path']
        history_table_path = output_config.get('history_path')
        partition_by = output_config.get('partition_by', [])
        data_format = output_config.get('format', 'parquet')
        mode = output_config.get('mode', 'overwrite')

        if not history_table_path:
            raise ValueError(
                "For SCD Type 4, 'history_path' must be specified in the output configuration.")

        df.write.format(data_format) \
            .mode(mode) \
            .partitionBy(partition_by) \
            .save(current_table_path)

        df_history = df.withColumn('record_timestamp', F.current_timestamp())
        df_history.write.format(data_format) \
            .mode('append') \
            .partitionBy(partition_by) \
            .save(history_table_path)
