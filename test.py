from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import udf
import random

# Tạo SparkSession
spark = SparkSession.builder.appName("Example").getOrCreate()

# Giả sử có một DataFrame ban đầu
data = [("Alice", 29), ("Bob", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Định nghĩa UDF sinh ngẫu nhiên giá trị 0 hoặc 1
def generate_random_json():
    return {"a": random.randint(0, 1), "b": random.randint(0, 1), "c": random.randint(0, 1)}

# Đăng ký UDF
generate_random_json_udf = udf(generate_random_json, StructType([
    StructField("a", IntegerType(), True),
    StructField("b", IntegerType(), True),
    StructField("c", IntegerType(), True)
]))

# Áp dụng UDF để tạo cột JSON ngẫu nhiên
df_with_json = df.withColumn("json_col", generate_random_json_udf())
df_with_json.printSchema()
# Hiển thị DataFrame với JSON ngẫu nhiên
df_with_json.show(truncate=False)
