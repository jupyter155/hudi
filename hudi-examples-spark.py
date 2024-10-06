from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import lit, rand, when, col, struct

# Tạo SparkSession
spark = SparkSession.builder.appName("Example").getOrCreate()

# Giả sử có một DataFrame ban đầu
data = [("Alice", 29), ("Bob", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Tạo các giá trị ngẫu nhiên 0 hoặc 1 sử dụng rand() và when()
df_random = df.withColumn("a", when(rand() > 0.5, 1).otherwise(0)) \
              .withColumn("b", when(rand() > 0.5, 1).otherwise(0)) \
              .withColumn("c", when(rand() > 0.5, 1).otherwise(0))

# Định nghĩa cấu trúc JSON
json_schema = StructType([
    StructField("a", IntegerType(), True),
    StructField("b", IntegerType(), True),
    StructField("c", IntegerType(), True)
])

# Tạo một cột với kiểu dữ liệu JSON từ các cột ngẫu nhiên
df_with_json = df_random.withColumn("json_col", struct(col("a"), col("b"), col("c")))
df_with_json.drop(col("a"),col("b"), col("c")).printSchema()
# Hiển thị DataFrame với cột JSON
df_with_json.select("name", "age", "json_col").show(truncate=False)
