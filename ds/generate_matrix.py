import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
import random

# configs
SPARK_MASTER = "192.168.80.55"
NAMENODE = "192.168.80.57"
HADOOP_PATH = f"hdfs://{NAMENODE}:9000/matrices"

# Worker configuration
NUM_WORKERS = 9
MEMORY_PER_WORKER = "6g"
CORES_PER_WORKER = 12

spark = SparkSession.builder \
    .appName("Generate Matrix") \
    .master(f"spark://{SPARK_MASTER}:7077") \
    .config("spark.executor.memory", MEMORY_PER_WORKER) \
    .config("spark.executor.memoryOverhead", "2g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .config("spark.executor.cores", str(CORES_PER_WORKER)) \
    .config("spark.default.parallelism", str(NUM_WORKERS * CORES_PER_WORKER)) \
    .config("spark.sql.shuffle.partitions", str(NUM_WORKERS * CORES_PER_WORKER * 2)) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

schema = StructType([
    StructField("row", IntegerType(), False),
    StructField("col", IntegerType(), False),
    StructField("value", DoubleType(), False)
])


def generate_matrix_partition(partition_id, total_partitions, size):
    """Generate a partition of matrix data with iterator to reduce memory"""
    elements_per_partition = (size * size) // total_partitions
    start_idx = partition_id * elements_per_partition
    end_idx = start_idx + elements_per_partition if partition_id < total_partitions - 1 else size * size

    # Use iterator instead of list to reduce memory
    for idx in range(start_idx, end_idx):
        i = idx // size
        j = idx % size
        value = random.uniform(0, 1000)
        yield i, j, value


for n in range(16, 17, 1):
    # Calculate optimal partitions based on matrix size
    matrix_size = 2 ** n
    num_elements = matrix_size * matrix_size

    bytes_per_row = 24
    target_partition_size_mb = 50
    target_rows_per_partition = (target_partition_size_mb * 1024 * 1024) // bytes_per_row

    # Calculate partitions
    min_partitions = NUM_WORKERS * CORES_PER_WORKER * 2
    optimal_partitions = max(min_partitions, num_elements // target_rows_per_partition)

    # Cap at reasonable maximum
    num_partitions = min(optimal_partitions, NUM_WORKERS * CORES_PER_WORKER * 10)

    print(f"\n{'=' * 60}")
    print(f"Matrix size: {matrix_size}x{matrix_size} (2^{n})")
    print(f"Total elements: {num_elements:,}")
    print(f"Using {num_partitions} partitions")
    print(f"~{num_elements // num_partitions:,} elements per partition")
    print(f"{'=' * 60}\n")

    output_path = f"{HADOOP_PATH}/matrices_{n}"

    # Generate and write matrix 1
    print(f"Generating matrix1 for n={n}...")
    partition_ids = spark.sparkContext.parallelize(range(num_partitions), num_partitions)

    matrix1_rdd = partition_ids.flatMap(
        lambda pid: generate_matrix_partition(pid, num_partitions, matrix_size)
    )
    df_matrix1 = spark.createDataFrame(matrix1_rdd, schema)

    df_matrix1.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"{output_path}/matrix1.parquet")

    print(f"✓ Matrix1 saved to {output_path}/matrix1.parquet")

    # Clear cache and unpersist to free memory
    spark.catalog.clearCache()

    # Generate and write matrix 2 (separate to reduce memory pressure)
    print(f"Generating matrix2 for n={n}...")
    partition_ids = spark.sparkContext.parallelize(range(num_partitions), num_partitions)

    matrix2_rdd = partition_ids.flatMap(
        lambda pid: generate_matrix_partition(pid, num_partitions, matrix_size)
    )
    df_matrix2 = spark.createDataFrame(matrix2_rdd, schema)

    df_matrix2.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"{output_path}/matrix2.parquet")

    print(f"✓ Matrix2 saved to {output_path}/matrix2.parquet")

    # Clear cache between iterations
    spark.catalog.clearCache()
    print(f"✓ Completed matrices_{n}\n")

spark.stop()
print("\n✓ Spark session stopped.")