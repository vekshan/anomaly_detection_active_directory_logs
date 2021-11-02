from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, last, udf
from pyspark.sql.types import DoubleType
import sys


def forward_fill(column_name):
    # define the window
    window = Window.partitionBy('host')\
                   .orderBy(col("time").cast('long'))\
                   .rangeBetween(-1800, Window.currentRow)

    # what column to fill forward
    return last(column_name, True).over(window)


def pivot(df):
    df = df.groupBy("time", "host", "total_events", "is_malicious").pivot(
        "event_id").sum("total_per_event")
    columns_to_fill = df.schema.names[4:]
    for column_name in columns_to_fill:
        df = df.withColumn('event_' + column_name, forward_fill(column_name))
    df = df.na.fill(value=0)
    df = df.drop(*columns_to_fill)
    df = df.distinct()
    return df


# def standardize(df):
#     """ Standardize before applying ML algorithms

#     Args:
#         df ([type]): [description]

#     Returns:
#         [type]: [description]
#     """
#     unlist = udf(lambda x: round(float(list(x)[0]), 3), DoubleType())
#     features = df.schema.names[4:]
#     for feature in features:
#         assembler = VectorAssembler(
#             inputCols=[feature], outputCol=feature+"_Vect")
#         scaler = MinMaxScaler(inputCol=feature+"_Vect",
#                               outputCol=feature+"_Scaled")
#         pipeline = Pipeline(stages=[assembler, scaler])
#         df = pipeline.fit(df).transform(df).withColumn(
#             feature+"_Scaled", unlist(feature+"_Scaled")).drop(feature+"_Vect")
#     return df


def main():
    df = spark.read.options(header='true', inferSchema='true') \
        .csv('../../data/processed/merged_dataset.csv')
    pivot_df = pivot(df)
    # pivot_df = standardize(pivot_df)
    pivot_df.coalesce(1).write.option("header", True).csv(
        "../../data/processed/merged_dataset_pivoted_")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "8g") \
        .appName('vb-app') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    main()
