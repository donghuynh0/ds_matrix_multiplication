from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Cấu hình giá đỗ xe
PARKING_FEE_PER_10_MINUTES = 5000  # 5,000 VNĐ / 10 phút

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("ParkingLotStatefulProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema cho dữ liệu từ Kafka
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("timestamp_unix", LongType(), True),
    StructField("license_plate", StringType(), True),
    StructField("location", StringType(), True),
    StructField("status_code", StringType(), True),
    StructField("entry_timestamp", LongType(), True)
])

# Đọc dữ liệu từ Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.80.57:9093") \
    .option("subscribe", "hw2") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON từ Kafka
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data") 
).select("data.*")

# Thêm cột event_time từ timestamp_unix
events_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp_unix"))
).withWatermark("event_time", "10 minutes")


print(events_df.show())

# # =============================================================================
# # STATEFUL AGGREGATION 1: Tính toán thời gian đỗ và phí cho từng xe
# # =============================================================================
# parking_state_df = events_df.groupBy(
#     col("license_plate"),
#     col("location")
# ).agg(
#     first("entry_timestamp").alias("entry_timestamp"),
#     last("status_code").alias("current_status"),
#     last("timestamp_unix").alias("last_update_timestamp"),
#     last("timestamp").alias("last_update_time")
# )

# # Tính thời gian đỗ (phút) và phí
# parking_with_fee_df = parking_state_df.withColumn(
#     "parked_duration_seconds",
#     col("last_update_timestamp") - col("entry_timestamp")
# ).withColumn(
#     "parked_duration_minutes",
#     (col("parked_duration_seconds") / 60).cast(IntegerType())
# ).withColumn(
#     "parked_blocks",
#     ceil(col("parked_duration_minutes") / 10).cast(IntegerType())
# ).withColumn(
#     "parking_fee",
#     col("parked_blocks") * lit(PARKING_FEE_PER_10_MINUTES)
# ).filter(
#     col("current_status") == "PARKED"
# )

# # =============================================================================
# # STATEFUL AGGREGATION 2: Thống kê vị trí trống và có xe
# # =============================================================================
# location_status_df = events_df.groupBy("location").agg(
#     last("status_code").alias("status"),
#     last("license_plate").alias("license_plate"),
#     last("timestamp").alias("last_update")
# )

# # Phân loại vị trí
# occupied_locations_df = location_status_df.filter(
#     col("status") == "PARKED"
# ).select(
#     col("location"),
#     col("license_plate"),
#     lit("Đang có xe").alias("availability_status")
# )

# available_locations_df = location_status_df.filter(
#     (col("status") != "PARKED") | (col("status").isNull())
# ).select(
#     col("location"),
#     lit(None).cast(StringType()).alias("license_plate"),
#     lit("Còn trống").alias("availability_status")
# )

# # Hợp nhất danh sách vị trí
# all_locations_df = occupied_locations_df.union(available_locations_df)

# # =============================================================================
# # STATEFUL AGGREGATION 3: Tổng hợp thống kê tổng quan
# # =============================================================================
# summary_df = events_df.groupBy(window(col("event_time"), "1 minute")).agg(
#     countDistinct(
#         when(col("status_code") == "PARKED", col("license_plate"))
#     ).alias("total_parked_vehicles"),
#     countDistinct(col("location")).alias("total_locations_used"),
#     count("*").alias("total_events")
# ).select(
#     col("window.start").alias("window_start"),
#     col("window.end").alias("window_end"),
#     col("total_parked_vehicles"),
#     col("total_locations_used"),
#     col("total_events")
# )

# # =============================================================================
# # OUTPUT 1: Chi tiết xe đang đỗ với thời gian và phí
# # =============================================================================
# def write_parked_vehicles(batch_df, batch_id):
#     print(f"\n{'='*80}")
#     print(f"BATCH {batch_id} - CHI TIẾT XE ĐANG ĐỖ")
#     print(f"{'='*80}")
    
#     batch_df.select(
#         "license_plate",
#         "location",
#         "parked_duration_minutes",
#         "parking_fee",
#         "last_update_time"
#     ).orderBy("location").show(100, truncate=False)
    
#     print(f"Tổng số xe đang đỗ: {batch_df.count()}")
#     print(f"{'='*80}\n")

# query1 = parking_with_fee_df.writeStream \
#     .outputMode("complete") \
#     .foreachBatch(write_parked_vehicles) \
#     .option("checkpointLocation", "/tmp/checkpoint_parked_vehicles") \
#     .start()

# # =============================================================================
# # OUTPUT 2: Danh sách vị trí (trống và có xe)
# # =============================================================================
# def write_location_status(batch_df, batch_id):
#     print(f"\n{'='*80}")
#     print(f"BATCH {batch_id} - TRẠNG THÁI VỊ TRÍ ĐỖ XE")
#     print(f"{'='*80}")
    
#     occupied = batch_df.filter(col("availability_status") == "Đang có xe").count()
#     available = batch_df.filter(col("availability_status") == "Còn trống").count()
    
#     print(f"\nTổng quan:")
#     print(f"  - Vị trí đang có xe: {occupied}")
#     print(f"  - Vị trí còn trống: {available}")
#     print(f"\nChi tiết vị trí đang có xe:")
    
#     batch_df.filter(col("availability_status") == "Đang có xe") \
#         .select("location", "license_plate", "availability_status") \
#         .orderBy("location") \
#         .show(60, truncate=False)
    
#     print(f"{'='*80}\n")

# query2 = all_locations_df.writeStream \
#     .outputMode("complete") \
#     .foreachBatch(write_location_status) \
#     .option("checkpointLocation", "/tmp/checkpoint_locations") \
#     .start()

# # =============================================================================
# # OUTPUT 3: Thống kê tổng quan theo thời gian
# # =============================================================================
# def write_summary(batch_df, batch_id):
#     print(f"\n{'='*80}")
#     print(f"BATCH {batch_id} - THỐNG KÊ TỔNG QUAN")
#     print(f"{'='*80}")
    
#     batch_df.select(
#         "window_start",
#         "window_end",
#         "total_parked_vehicles",
#         "total_locations_used",
#         "total_events"
#     ).orderBy("window_start", ascending=False).show(10, truncate=False)
    
#     print(f"{'='*80}\n")

# query3 = summary_df.writeStream \
#     .outputMode("complete") \
#     .foreachBatch(write_summary) \
#     .option("checkpointLocation", "/tmp/checkpoint_summary") \
#     .start()

# # Chờ tất cả các query
# try:
#     print("Spark Streaming đang chạy. Nhấn Ctrl+C để dừng...")
#     spark.streams.awaitAnyTermination()
# except KeyboardInterrupt:
#     print("\nĐang dừng Spark Streaming...")
#     query1.stop()
#     query2.stop()
#     query3.stop()
#     spark.stop()
#     print("Đã dừng Spark Streaming.")