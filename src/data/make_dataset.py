from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, create_map, lit
from pyspark.sql.types import IntegerType
from itertools import cycle, islice, chain


def calculate_window(df):
    """[summary]

    Args:
        df ([type]): [description]

    Returns:
        [type]: [description]
    """
    events_to_keep = [1, 3, 8, 10, 11, 12, 4624, 4625, 4648,
                      4658, 4661, 4663, 4672, 4698, 4768, 5140, 5145, 5156, 5158]
    df = df.filter(df.event_id.isin(events_to_keep))

    w_all = (Window.partitionBy('host').orderBy(
        F.col("time").cast('long')).rangeBetween(-1800, Window.currentRow))
    df = df.withColumn('total_events', F.count("time").over(w_all))

    w_per_event = (Window.partitionBy('host', 'event_id').orderBy(
        F.col("time").cast('long')).rangeBetween(-1800, Window.currentRow))
    df = df.withColumn('total_per_event', F.count("time").over(w_per_event))

    return df.distinct().orderBy("time", "host")


def process_lanl():
    """[summary]

    Returns:
        [type]: [description]
    """
    path = "../../data/raw/wls_day-01"
    df = spark.read.json(path)
    hosts_to_keep = ['Comp876177', 'Comp337732', 'Comp581624', 'Comp158828']
    df = df.select("Time", "LogHost", "EventID").filter(
        df.LogHost.isin(hosts_to_keep))
    df = df.withColumn("is_malicious", lit(0))
    new_columns = ['time', 'host', 'event_id', 'is_malicious']
    return df.toDF(*new_columns)


def process_evtx():
    """This functions reads and selects required columns from the
    path for evtx dataset

    Args:
        path (string): The path of the json dataset
    """
    path = "../../data/raw/evtx_data.csv"
    df = spark.read.options(header='true', inferSchema='true') \
        .csv(path)
    df = df.select("SystemTime", "Computer", "EventID")
    convert_to_unix_udf = udf(convert_to_unix, IntegerType())
    df = df.withColumn("SystemTime", convert_to_unix_udf(df.SystemTime))

    hosts_to_keep = ['Comp876177', 'Comp337732', 'Comp581624', 'Comp158828']
    current_hosts = df.select(F.collect_set(
        'Computer').alias('Computer')).first()['Computer']
    hosts_final = list(islice(cycle(hosts_to_keep), len(current_hosts)))

    host_mapping = dict(zip(current_hosts, hosts_final))
    mapping_expr = create_map([lit(x) for x in chain(*host_mapping.items())])
    df = df.withColumn('Computer', mapping_expr[df['Computer']])

    df = df.withColumn("is_malicious", lit(1))

    new_columns = ['time', 'host', 'event_id', 'is_malicious']

    return df.toDF(*new_columns)


def convert_to_unix(event_time):
    time = event_time[10:]
    time_list = time.split(":")
    return int(time_list[0])*3600+int(time_list[1])*60+int(float(time_list[2]))


def process_purplesharp():
    """This functions reads and selects required columns from the
    path for malicious datasets

    Args:
        path (string): The path of the json dataset
    """
    path = "../../data/raw/purplesharp_ad_playbook_I_2020-10-22042947.json"
    df = spark.read.json(path)
    df = df.select("EventTime", "host", "EventID")

    convert_to_unix_udf = udf(convert_to_unix, IntegerType())
    df = df.withColumn("EventTime", convert_to_unix_udf(df.EventTime))

    new_columns = ['time', 'host', 'event_id']
    return df.toDF(*new_columns)


def main():
    benign_df = process_lanl()
    # benign_df = calculate_window(benign_df)
    # benign_df.coalesce(1).write.csv("../../data/processed/benign_1.csv")
    # purplesharp_df = process_purplesharp()
    # purplesharp_df = calculate_window(purplesharp_df)
    # purplesharp_df.coalesce(1).write.option(
    #     "header", True).csv("../../data/processed/purplesharp")
    evtx_df = process_evtx()
    merged_df = benign_df.unionByName(evtx_df)
    final_df = calculate_window(merged_df)
    final_df.coalesce(1).write.option(
        "header", True).csv("../../data/processed/merged")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "8g") \
        .appName('vb-app') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    main()
