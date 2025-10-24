from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import col, to_date, countDistinct, regexp_extract # type: ignore

def read_hdfs(url):
    df = spark.read.parquet(url)
    return df

def show(df):
    df.show(truncate=False)
    
def process_raw_data(file_path):
    rdd = spark.sparkContext.textFile(file_path)
    df_raw = rdd.map(lambda x: (x, )).toDF(["raw_line"])

    df = df_raw.select(
        regexp_extract("raw_line", r"\[(.*?)\]", 1).alias("timestamp"),
        regexp_extract("raw_line", r"user(\d+)", 1).alias("user_id"),
        regexp_extract("raw_line", r"(purchased|added to cart|viewed)", 1).alias("action"),
        regexp_extract("raw_line", r"([A-Za-z]+)\s+x\d+", 1).alias("product"),
        regexp_extract("raw_line", r"x(\d+)", 1).cast("int").alias("quantity"),
        regexp_extract("raw_line", r"\$(\d+\.\d+)", 1).cast("double").alias("price"),
        regexp_extract("raw_line", r"\$\d+\.\d+\.\s*(.*)$", 1).alias("note")
    )
    return df

def write_hdfs(df, url):
    df.write.mode("overwrite").parquet(f"{url}")
    print("\n\n Successfully wrote to HDFS \n\n'")

def write_sql(df, MYSQL_URL, table):
    df.write \
            .format("jdbc") \
            .option("url", MYSQL_URL) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table) \
            .option("user", "root") \
            .option("password", "bemo1806") \
            .mode("overwrite") \
            .save()
        
    print("\n\n Successfully wrote to SQL \n\n'")
    
    
def write_cassandra(df, keyspace, table):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=table, keyspace=keyspace) \
        .save()
    
    print(f"\n\n Successfully wrote to Cassandra \n\n")


MASTER_SPARK = "192.168.80.55:7077"
NAMENODE = "192.168.80.57"
MYSQL_URL = "jdbc:mysql://192.168.80.55:3306/bigdata"



file_path = f"hdfs://{NAMENODE}:9000/bigdata/hw1/sales_log_unstructured.csv"

name_app = "write_sql"

# create a spark session 
spark = SparkSession.builder \
    .appName(f"{name_app}") \
    .master("spark://192.168.80.55:7077") \
    .config("spark.jars", "/home/donghuynh0/spark-3.5.7/jars/mysql-connector-j-8.1.0.jar") \
    .config("spark.jars", "/home/donghuynh0/spark-3.5.7/jars/spark-cassandra-connector-assembly_2.12-3.4.1.jar") \
    .config("spark.cassandra.connection.host", "192.168.80.57") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()


# df = process_raw_data(file_path)
# write_hdfs(df, "hdfs://192.168.80.57:9000/bigdata/hw1/structured_sales")


structured_url = "hdfs://192.168.80.57:9000/bigdata/hw1/structured_sales"
df = read_hdfs(structured_url)


# part 01
purchases = df.filter(col("action") == "purchased")
purchases = purchases.withColumn("date", to_date(col("timestamp")))
daily_users = purchases.groupBy("date", "product") \
    .agg(countDistinct("user_id").alias("num_users")) \
    .orderBy("date", "product")


# write_hdfs(daily_users, "hdfs://192.168.80.57:9000/bigdata/hw1/result_01")
# write_sql(daily_users, MYSQL_URL, "daily_users")
write_cassandra(daily_users, 'bigdata', 'daily_users')

# part 02
# not_purchases = df.filter(col("action") != "purchased")

# write_hdfs(not_purchases, "hdfs://192.168.80.57:9000/bigdata/hw1/result_02")
# write_sql(not_purchases, MYSQL_URL, "not_purchases")
# write_cassandra(not_purchases, 'bigdata', 'not_purchases')

spark.stop()