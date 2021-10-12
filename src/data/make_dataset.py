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
    w_all = (Window.partitionBy('UserName').orderBy(
        F.col("Time").cast('long')).rangeBetween(-3600, Window.currentRow))
    df = df.withColumn('total_events', F.count("Time").over(w_all))

    w_per_event = (Window.partitionBy('UserName', 'EventID').orderBy(
        F.col("Time").cast('long')).rangeBetween(-3600, Window.currentRow))
    df = df.withColumn('total_per_event', F.count("Time").over(w_per_event))

    new_columns = ['time', 'username', 'event_id',
                   'total_events', 'total_per_event']
    df = df.toDF(*new_columns)

    return df.select(*new_columns).distinct().orderBy("Time")


def process_benign():
    path = "../../data/raw/wls_day-01"
    df = spark.read.json(path)
    return df.select("Time", "UserName", "EventID")


def process_malicious(path):
    """This functions reads and selects required columns from the 
    path for malicious datasets

    Args:
        path (string): The path of the json dataset
    """
    pass


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
    spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "8g") \
        .appName('vb-app') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    main()
