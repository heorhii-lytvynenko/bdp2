from map import build_parsed
from red import merge_values, print_result
from Sort import sort_result


INPUT_PATH = "duom_cut.txt"


def get_spark_context():
    try:
        return sc
    except NameError:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("MarsrutasAggregation").getOrCreate()
        return spark.sparkContext


def run(input_path=INPUT_PATH, spark_context=None):
    if spark_context is None:
        spark_context = get_spark_context()

    rawdata = spark_context.textFile(input_path)
    parsed = build_parsed(rawdata)
    result = parsed.reduceByKey(merge_values)
    visos_zonos, rows = sort_result(parsed, result)
    print_result(rows, visos_zonos)
    return rows


if __name__ == "__main__":
    run()
