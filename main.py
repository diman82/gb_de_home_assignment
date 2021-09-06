import os

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.functions import lit
from pyspark.sql.functions import when
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


def page_loads_per_session():
    pass


def data_loading_and_structuring():
    findspark.init()
    findspark.find()

    spark = SparkSession \
        .builder \
        .config("spark.driver.memory", "15g") \
        .config("spark.driver.maxResultSize ", "0") \
        .appName('gb_de_home_assignment') \
        .getOrCreate()

    print("loading parquet files")
    sessions_df = spark.read.option("badRecordsPath", os.path.join(rootpath.detect(), 'data', 'badRecordsPath')) \
        .parquet(os.path.join(rootpath.detect(), 'data', 'sessions.snappy.parquet'), header=True)
    # sessions_pd_df = pd.read_parquet(os.path.join(rootpath.detect(), 'data', 'sessions.snappy.parquet'))
    # client_flows_df = spark.read.option("badRecordsPath", os.path.join(rootpath.detect(), 'data', 'badRecordsPath')) \
    #     .parquet(os.path.join(rootpath.detect(), 'data', 'client_flows.snappy.parquet'), header=False)
    # page_loads_df = spark.read.option("badRecordsPath", os.path.join(rootpath.detect(), 'data', 'badRecordsPath')) \
    #     .parquet(os.path.join(rootpath.detect(), 'data', 'page_loads.snappy.parquet'), header=False)
    # user_action_df = spark.read.option("badRecordsPath", os.path.join(rootpath.detect(), 'data', 'badRecordsPath')) \
    #     .parquet(os.path.join(rootpath.detect(), 'data', 'user_action.snappy.parquet'), header=False)

    # flatmap the 'struggle_score_types' field, containing array of ditionaries
    schema = StructType([StructField('struggle_score_rage_click', StringType(), True),
                         StructField('struggle_score_zoom', StringType(), True),
                         StructField('struggle_score_http_error', StringType(), True),
                         StructField('struggle_score_dead_click', StringType(), True),
                         StructField('struggle_score_app_crash', StringType(), True),
                         StructField('struggle_score_http_response_time', StringType(), True),
                         StructField('struggle_score_too_many_tilts', StringType(), True)
                         ])

    res_df = spark.createDataFrame(data=sessions_df.select("struggle_score_types").rdd, schema=schema)
    res_df = res_df.toDF(*res_df.columns)
    sessions_df.drop("struggle_score_types").collect()

    w = Window.orderBy(monotonically_increasing_id())
    sessions_df = sessions_df.withColumn("row_index", row_number().over(w))
    res_df = res_df.withColumn("row_index", row_number().over(w))

    sessions_df = sessions_df.join(res_df, on=["row_index"]).drop("row_index")
    # new_df = sessions_df.union(res_df)


    cond = [dataset_df.plc_name == metadata_df.plc_name]
    inner_join_df = dataset_df \
        .join(metadata_df, cond, 'inner')

    # rename duplicate column names: https://stackoverflow.com/questions/52624888/how-to-write-dataframe-with-duplicate-column-name-into-a-csv-file-in-pyspark
    newNames = ['name', 'time', 'value', 'site_name', 'plc_name', 'sub_system_name', 'reading_type',
                'reading_attribute',
                'machine_type', 'machine_vendor', 'machine_serial_number', 'machine_production_date',
                'plc_name_duplicate']
    inner_join_df = inner_join_df.toDF(*newNames)

    print(dataset_df.take(10))
    print(metadata_df.take(10))
    print(inner_join_df.take(10))
    inner_join_df.collect()

    inner_join_df.coalesce(1).write.mode('overwrite') \
        .option('header', 'true') \
        .parquet("result.parquet")

    # inner_join_df.write.mode('overwrite') \
    #     .option('header', 'true') \
    #     .parquet("result.parquet")  # write supporting HDFS


if __name__ == "__main__":
    data_loading_and_structuring()
