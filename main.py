import os

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number, count, lit, when, col, sum, max, min
import findspark
import rootpath


def shakespeare_word_count():
    findspark.init()
    conf = SparkConf().setAppName('MyFirstStandaloneApp')
    sc = SparkContext(conf=conf)

    print("loading text file")
    text_file = sc.textFile("./shakespeare.txt")

    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)

    print("Number of elements: " + str(counts.count()))
    counts.saveAsTextFile("./shakespeareWordCount")


def data_loading():
    findspark.init()
    findspark.find()

    global spark
    spark = SparkSession \
        .builder \
        .config("spark.driver.memory", "15g") \
        .config("spark.driver.maxResultSize ", "0") \
        .appName('gb_de_home_assignment') \
        .getOrCreate()

    print("loading parquet files")
    global client_flows_df
    client_flows_df = spark.read.option("badRecordsPath", os.path.join(rootpath.detect(), 'data', 'badRecordsPath')) \
        .parquet(os.path.join(rootpath.detect(), 'data', 'client_flows.snappy.parquet'), header=True)
    global session_df
    session_df = spark.read.option("badRecordsPath", os.path.join(rootpath.detect(), 'data', 'badRecordsPath')) \
        .parquet(os.path.join(rootpath.detect(), 'data', 'sessions.snappy.parquet'), header=True)
    global page_loads_df
    page_loads_df = spark.read.option("badRecordsPath", os.path.join(rootpath.detect(), 'data', 'badRecordsPath')) \
        .parquet(os.path.join(rootpath.detect(), 'data', 'page_loads.snappy.parquet'), header=True)
    # global user_action_df
    # user_action_df = spark.read.option("badRecordsPath", os.path.join(rootpath.detect(), 'data', 'badRecordsPath')) \
    #     .parquet(os.path.join(rootpath.detect(), 'data', 'user_action.snappy.parquet'), header=True)


def transformations_and_aggregations():
    client_flows_df.groupBy("session_uuid")\
        .agg(count("successful").alias("client_flows_per_session"),
             sum("click_count").alias("click_count_per_session"),
             sum("click_repetition_count").alias("click_count_per_session"),
             sum("field_change_count").alias("field_change_count_per_session"),
             sum("field_change_repetitions").alias("field_change_repetition_per_session"),
             max("close_reason").alias("most_common_close_reason_per_session"),
             ) \
        .show(truncate=False)
    client_flows_df.groupBy("session_uuid")\
        .agg(count("successful").alias("successful_true"),
             sum("click_count").alias("total_click_count_per_session"),
             sum("click_repetition_count").alias("total_click_count_per_session"),
             sum("field_change_count").alias("total_field_change_count_per_session"),
             sum("field_change_repetitions").alias("total_field_change_repetition_per_session"),
             max("close_reason").alias("most_common_close_reason_per_session"),
             ) \
        .where(col("successful_true") == True) \
        .show(truncate=False)
    client_flows_df.groupBy("session_uuid") \
        .agg(count("successful").alias("successful_false"),
             sum("click_count").alias("total_click_count_per_session"),
             sum("click_repetition_count").alias("total_click_count_per_session"),
             sum("field_change_count").alias("total_field_change_count_per_session"),
             sum("field_change_repetitions").alias("total_field_change_repetition_per_session"),
             max("close_reason").alias("most_common_close_reason_per_session"),
             ) \
        .where(col("successful_false") == False) \
        .show(truncate=False)


    data_counts = page_loads_df.groupBy("session_uuid") \
        .agg(count("hit_ts").alias("page_loads_per_session"),
             max("struggle_score").alias("most_common_close_reason_per_session"),
             min("struggle_score").alias("most_common_close_reason_per_session"),
             )
    page_loads_joined_df = page_loads_df.join(data_counts, "session_uuid").dropDuplicates()
    page_loads_joined_df.show(truncate=False)

    # flatmap the 'struggle_score_types' field, containing array of ditionaries
    schema = StructType([StructField('struggle_score_rage_click', StringType(), True),
                         StructField('struggle_score_zoom', StringType(), True),
                         StructField('struggle_score_http_error', StringType(), True),
                         StructField('struggle_score_dead_click', StringType(), True),
                         StructField('struggle_score_app_crash', StringType(), True),
                         StructField('struggle_score_http_response_time', StringType(), True),
                         StructField('struggle_score_too_many_tilts', StringType(), True)
                         ])

    res_df = spark.createDataFrame(data=session_df.select("struggle_score_types").rdd, schema=schema)
    res_df.printSchema()
    session_df.drop("struggle_score_types")

    w = Window.orderBy(monotonically_increasing_id())
    sessions_df = session_df.withColumn("row_index", row_number().over(w))
    res_df = res_df.withColumn("row_index", row_number().over(w))

    sessions_df = sessions_df.join(res_df, on=["row_index"]).drop("row_index")
    print(sessions_df.columns)


    cond = [dataset_df.plc_name == metadata_df.plc_name]
    inner_join_df = dataset_df \
        .join(metadata_df, cond, 'inner')


    inner_join_df.coalesce(1).write.mode('overwrite') \
        .option('header', 'true') \
        .parquet("result.parquet")

    # inner_join_df.write.mode('overwrite') \
    #     .option('header', 'true') \
    #     .parquet("result.parquet")  # write supporting HDFS


if __name__ == "__main__":
    data_loading()
    transformations_and_aggregations()
