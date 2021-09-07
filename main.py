import os

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number, count, col, sum, max, min, explode
import findspark
import rootpath


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
    def flatmap_struggle_score_types(x):
        temp_dict = dict()
        for i in range(7):
            try:
                temp_dict[i] = x.struggle_score_types[i]
            except IndexError:
                temp_dict[i] = {'temp_key': '0.0'}
        return (temp_dict[0],
                temp_dict[1],
                temp_dict[2],
                temp_dict[3],
                temp_dict[4],
                temp_dict[5],
                temp_dict[6])

    # flatmap the 'struggle_score_types' field, containing array of ditionaries
    struggle_score_types_df = session_df.rdd.map(flatmap_struggle_score_types) \
        .toDF(["struggle_score_rage_click", "struggle_score_zoom",
               "struggle_score_http_error", "struggle_score_dead_click",
               "struggle_score_app_crash", "struggle_score_http_response_time", "struggle_score_too_many_tilts"])

    struggle_score_types_df.printSchema()
    struggle_score_types_df.show()

    session_df.drop("struggle_score_types")

    # join sessions_df with struggle_score_types_df
    w = Window.orderBy(monotonically_increasing_id())
    sessions_df = session_df.withColumn("row_index", row_number().over(w))
    res_df = struggle_score_types_df.withColumn("row_index", row_number().over(w))

    sessions_df = sessions_df.join(res_df, on=["row_index"]).drop("row_index")
    print(sessions_df.columns)

    client_flows_df.groupBy("session_uuid") \
        .agg(count("successful").alias("client_flows_per_session"),
             sum("click_count").alias("click_count_per_session"),
             sum("click_repetition_count").alias("click_count_per_session"),
             sum("field_change_count").alias("field_change_count_per_session"),
             sum("field_change_repetitions").alias("field_change_repetition_per_session"),
             max("close_reason").alias("most_common_close_reason_per_session"),
             ) \
        .show(truncate=False)
    client_flows_df.groupBy("session_uuid") \
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

    cond1 = [sessions_df.session_uuid == client_flows_df.session_uuid]
    first_join_df = sessions_df \
        .join(client_flows_df, cond1, 'inner')

    cond2 = [first_join_df.session_uuid == page_loads_joined_df.session_uuid]
    final_res_df = sessions_df \
        .join(page_loads_joined_df, cond2, 'inner')

    final_res_df.printSchema()

    final_res_df.coalesce(1).write.mode('overwrite') \
        .option('header', 'true') \
        .parquet("result.parquet")

    # inner_join_df.write.mode('overwrite') \
    #     .option('header', 'true') \
    #     .parquet("result.parquet")  # write supporting HDFS


def run_spark_job():
    data_loading()
    transformations_and_aggregations()


if __name__ == "__main__":
    run_spark_job()
