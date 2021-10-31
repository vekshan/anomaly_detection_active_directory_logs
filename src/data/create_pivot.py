from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

def process_benign():
    path = "../../data/processed/benign_with_rolling_window.csv"
    df = spark.read.csv(path).toDF(
        "time", "username", "event_id", "total_events", "total_per_event"
        )
    keep_event_ids = [4624, 4625, 4627, 4648, 4658, 4661, 
                      4672, 4697, 4698, 4768, 4779, 5140, 
                      5145, 5158]
    df = df.withColumn("event_id", df["event_id"].cast(IntegerType()))
    df = df.withColumn("total_events", df["total_events"].cast(IntegerType()))
    df = df.withColumn("total_per_event", df["total_per_event"].cast(IntegerType()))
    df = df.filter(df.event_id.isin(keep_event_ids))
    return df

def pivot(df):
    df = df.groupBy("time", "username", "total_events").pivot("event_id").sum("total_per_event")
    df = df.na.fill(value=0, subset=["4624", "4625", "4648", "4672", "4768"])
    return df

def standardize(df):
    unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())
    for i in ["total_events", "4624", "4625", "4648", "4672", "4768"]:
        assembler = VectorAssembler(inputCols=[i], outputCol=i+"_Vect")
        scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")
        pipeline = Pipeline(stages=[assembler, scaler])
        df = pipeline.fit(df).transform(df).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")
    return df
    
def main():
    pivot_df = process_benign()
    pivot_df = pivot(pivot_df)
    pivot_df = standardize(pivot_df)
    pivot_df.coalesce(1).write.option("header",True).csv("../../data/processed/pivot_with_standardize.csv")
    
if __name__ == "__main__":
    spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "8g") \
        .appName('vb-app') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    main()