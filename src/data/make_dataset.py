from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F


def calculate_window(df):
    """[summary]

    Args:
        df ([type]): [description]

    Returns:
        [type]: [description]
    """
    w_all = (Window.partitionBy('host').orderBy(
        F.col("time").cast('long')).rangeBetween(-3600, Window.currentRow))
    df = df.withColumn('total_events', F.count("time").over(w_all))

    w_per_event = (Window.partitionBy('host', 'event_id').orderBy(
        F.col("time").cast('long')).rangeBetween(-3600, Window.currentRow))
    df = df.withColumn('total_per_event', F.count("time").over(w_per_event))

    return df.distinct().orderBy("time", "host")


def process_benign():
    """[summary]

    Returns:
        [type]: [description]
    """
    path = "../../data/raw/wls_day-01"
    df = spark.read.json(path)
    df = df.select("Time", "UserName", "EventID")
    new_columns = ['time', 'host', 'event_id']
    return df.toDF(*new_columns)


def process_malicious(path):
    """This functions reads and selects required columns from the
    path for malicious datasets

    Args:
        path (string): The path of the json dataset
    """
    path = "../data/raw/evtx_data.csv"
    df = spark.read.options(header='true', inferSchema='true') \
        .csv(path)
    df = df.select("SystemTime", "Computer", "EventID")
    new_columns = ['time', 'host', 'event_id']
    return df.toDF(*new_columns)


def concatenate(benign_df, malicious_dfs):
    """This function concatenates malicious datasets to the benign
    dataset.

    Args:
        benign_df (dataframe): [description]
        malicious_dfs (list(dataframe)): [description]
    """
    pass


def main():
    benign_df = process_benign()
    benign_df = calculate_window(benign_df)
    benign_df.coalesce(1).write.csv("../../data/processed/benign_1.csv")


if __name__ == "__main__":
    spark = SparkSession.builder
    .master('local[*]')
    .config("spark.driver.memory", "8g")
    .appName('vb-app')
    .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    main()
