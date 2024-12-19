from common.transformation_registry import TransformationRegistry
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Dict, Any, Tuple
import json


@TransformationRegistry.register()
def select_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """Select specific columns from DataFrame"""
    return df.select(*columns)


@TransformationRegistry.register()
def rename_columns(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    """
    Rename columns in a Spark DataFrame using withColumnRenamed

    Args:
        df (DataFrame): Input Spark DataFrame
        mapping (Dict[str, str]): Dictionary mapping old column names to new names

    Returns:
        DataFrame: DataFrame with renamed columns
    """
    result_df = df
    for old_name, new_name in mapping.items():
        result_df = result_df.withColumnRenamed(old_name, new_name)
    return result_df


@TransformationRegistry.register()
def cast_columns(df: DataFrame, type_mapping: Dict[str, str]) -> DataFrame:
    """Cast columns to specified types"""
    for col_name, new_type in type_mapping.items():
        df = df.withColumn(col_name, F.col(col_name).cast(new_type))
    return df


@TransformationRegistry.register()
def filter_rows(df: DataFrame, condition: str) -> DataFrame:
    """Filter rows based on SQL-like condition"""
    return df.filter(condition)


@TransformationRegistry.register()
def add_columns(df: DataFrame, columns: Dict[str, str]) -> DataFrame:
    """Add new columns based on expressions"""
    for col_name, expression in columns.items():
        df = df.withColumn(col_name, F.expr(expression))
    return df


@TransformationRegistry.register()
def drop_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """Drop specified columns"""
    return df.drop(*columns)


@TransformationRegistry.register()
def drop_duplicates(df: DataFrame, subset: List[str] = None) -> DataFrame:
    """Remove duplicate rows"""
    return df.dropDuplicates(subset=subset)


@TransformationRegistry.register()
def sort_dataframe(df: DataFrame, columns: List[str], ascending: bool = True) -> DataFrame:
    """Sort DataFrame by specified columns"""
    return df.orderBy(*[F.col(c).asc() if ascending else F.col(c).desc() for c in columns])


@TransformationRegistry.register()
def group_and_aggregate(
    df: DataFrame,
    group_by: List[str],
    aggregations: Dict[str, str]
) -> DataFrame:
    """
    Group by columns and perform aggregations
    aggregations format: {"new_col_name": "agg_func(col_name)"}
    """
    df = df.groupBy(*group_by)
    aggs = [F.expr(expr).alias(col) for col, expr in aggregations.items()]
    return df.agg(*aggs)


@TransformationRegistry.register()
def join_dataframes(
    df1: DataFrame,
    df2: DataFrame,
    join_cols: List[str],
    join_type: str = "inner"
) -> DataFrame:
    """Join two DataFrames"""
    return df1.join(df2, join_cols, join_type)


@TransformationRegistry.register()
def split_column_to_rows(df: DataFrame, column: str, delimiter: str = ",") -> DataFrame:
    """Split a column containing delimited values into multiple rows."""
    return df.withColumn(column, F.explode(F.split(F.col(column), delimiter)))


@TransformationRegistry.register()
def extract_df(df: DataFrame, split_column: str, id_columns: List[str], dedup_columns: List[str]) -> Tuple[DataFrame, DataFrame]:
    """
    Separates a DataFrame into two tables: metadata and split column values.

    Parameters:
    df (DataFrame): The input DataFrame.
    split_column (str): The column to split and normalize.
    id_columns (List[str]): Columns that uniquely identify each record.
    dedup_columns (List[str]): Columns to use for deduplication in metadata table.

    Returns:
    Tuple[DataFrame, DataFrame]: The metadata DataFrame and the split column DataFrame.
    """

    metadata_columns = [col for col in df.columns if col != split_column]
    metadata_df = df.select(metadata_columns).dropDuplicates(dedup_columns)

    split_column_df = df.select(
        *id_columns, split_column).dropDuplicates(dedup_columns + [split_column])

    split_column_df = split_column_df.withColumn(
        "value", F.explode(F.split(F.col(split_column), ","))
    ).select(*id_columns, "value").dropDuplicates()

    split_column_df = split_column_df.withColumn(
        "id", F.monotonically_increasing_id())

    return metadata_df, split_column_df


@TransformationRegistry.register()
def extract_unique_entities(
    df: DataFrame,
    entity_columns: List[str],
    id_column: str,
    drop_original_columns: bool = True,
    drop_after_join: List[str] = None
) -> Tuple[DataFrame, DataFrame]:
    """
    Extracts unique entities from specified columns and creates a separate DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        entity_columns (List[str]): Columns representing the entity to extract.
        id_column (str): Name of the identifier column for the entity.
        drop_original_columns (bool): Whether to drop the original entity columns from the input DataFrame.
        drop_after_join (List[str]): Specific columns to drop after the entity join (optional).

    Returns:
        Tuple[DataFrame, DataFrame]: A tuple containing:
            - The modified input DataFrame with a foreign key to the new entity.
            - The new entity DataFrame with unique entities.
    """
    # Extract unique entities
    entity_df = df.select(*entity_columns).dropDuplicates()

    # If the entity does not have a unique identifier, create one
    if id_column not in entity_columns:
        entity_df = entity_df.withColumn(
            id_column, F.monotonically_increasing_id()
        )

    # Join the new entity ID back to the original DataFrame
    df_with_id = df.join(entity_df, on=entity_columns, how='left')

    # Drop the original entity columns if specified
    if drop_original_columns:
        df_with_id = df_with_id.drop(*entity_columns)

    # Drop specific columns after the join, if requested
    if drop_after_join:
        df_with_id = df_with_id.drop(*drop_after_join)

    return df_with_id, entity_df


@TransformationRegistry.register()
def extract_transitive_dependencies(
    df: DataFrame,
    determinant_columns: List[str],
    dependent_columns: List[str],
    new_table_name: str,
    drop_original_columns: bool = True
) -> Tuple[DataFrame, DataFrame]:
    """
    Extracts transitive dependencies into a separate DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        determinant_columns (List[str]): Columns that determine the dependent columns.
        dependent_columns (List[str]): Columns that are transitively dependent.
        new_table_name (str): Name for the new DataFrame containing the transitive dependency.
        drop_original_columns (bool): Whether to drop the dependent columns from the input DataFrame.

    Returns:
        Tuple[DataFrame, DataFrame]: The modified input DataFrame and the new DataFrame with transitive dependencies.
    """
    # Extract unique combinations of determinant and dependent columns
    transitive_df = df.select(*determinant_columns,
                              *dependent_columns).dropDuplicates()

    # Optionally generate a unique key if determinant_columns are not unique identifiers
    determinant_key = new_table_name + "_id"
    transitive_df = transitive_df.withColumn(
        determinant_key, F.monotonically_increasing_id())

    # Join the new key back to the original DataFrame
    df_with_key = df.join(
        transitive_df.select(*determinant_columns, determinant_key),
        on=determinant_columns,
        how='left'
    )

    if drop_original_columns:
        df_with_key = df_with_key.drop(*dependent_columns)

    return df_with_key, transitive_df
