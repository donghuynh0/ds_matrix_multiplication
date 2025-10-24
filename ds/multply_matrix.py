import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql.functions import col, sum as spark_sum, coalesce, lit
from datetime import datetime

# Configs
SPARK_MASTER = "192.168.80.55"
NAMENODE = "192.168.80.57"
HADOOP_PATH = f"hdfs://{NAMENODE}:9000/matrices"
RESULTS_PATH = f"hdfs://{NAMENODE}:9000/results"

# Worker configuration
NUM_WORKERS = 6
MEMORY_PER_WORKER = "6g"
CORES_PER_WORKER = 12

# Strassen configuration
STRASSEN_THRESHOLD = 256  # Higher threshold to reduce recursion depth
MAX_DEPTH = 4  # Limit recursion depth to prevent port exhaustion


spark = SparkSession.builder \
    .appName("Multiply Matrix") \
    .master(f"spark://{SPARK_MASTER}:7077") \
    .config("spark.executor.memory", MEMORY_PER_WORKER) \
    .config("spark.executor.memoryOverhead", "2g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .config("spark.executor.cores", str(CORES_PER_WORKER)) \
    .config("spark.default.parallelism", str(NUM_WORKERS * CORES_PER_WORKER)) \
    .config("spark.sql.shuffle.partitions", str(NUM_WORKERS * CORES_PER_WORKER * 2)) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("row", IntegerType(), False),
    StructField("col", IntegerType(), False),
    StructField("value", DoubleType(), False)
])


def standard_multiply(df_a, df_b, size):
    """Standard matrix multiplication for small matrices or base case"""
    # Join on df_a.col == df_b.row
    df_joined = df_a.alias("a").join(
        df_b.alias("b"),
        col("a.col") == col("b.row"),
        "inner"
    ).select(
        col("a.row").alias("row"),
        col("b.col").alias("col"),
        (col("a.value") * col("b.value")).alias("product")
    )
    
    # Sum products for each (row, col) pair
    df_result = df_joined.groupBy("row", "col").agg(
        spark_sum("product").alias("value")
    )
    
    return df_result


def add_matrices(df_a, df_b):
    """Add two matrices element-wise"""
    df_result = df_a.alias("a").join(
        df_b.alias("b"),
        (col("a.row") == col("b.row")) & (col("a.col") == col("b.col")),
        "full_outer"
    ).select(
        coalesce(col("a.row"), col("b.row")).alias("row"),
        coalesce(col("a.col"), col("b.col")).alias("col"),
        (coalesce(col("a.value"), lit(0.0)) + coalesce(col("b.value"), lit(0.0))).alias("value")
    )
    return df_result


def subtract_matrices(df_a, df_b):
    """Subtract matrix B from matrix A element-wise"""
    df_result = df_a.alias("a").join(
        df_b.alias("b"),
        (col("a.row") == col("b.row")) & (col("a.col") == col("b.col")),
        "full_outer"
    ).select(
        coalesce(col("a.row"), col("b.row")).alias("row"),
        coalesce(col("a.col"), col("b.col")).alias("col"),
        (coalesce(col("a.value"), lit(0.0)) - coalesce(col("b.value"), lit(0.0))).alias("value")
    )
    return df_result


def split_matrix(df, size):
    """Split matrix into 4 quadrants"""
    half_size = size // 2
    
    # Top-left (A11)
    a11 = df.filter((col("row") < half_size) & (col("col") < half_size))
    
    # Top-right (A12)
    a12 = df.filter((col("row") < half_size) & (col("col") >= half_size)) \
        .select(col("row"), (col("col") - half_size).alias("col"), col("value"))
    
    # Bottom-left (A21)
    a21 = df.filter((col("row") >= half_size) & (col("col") < half_size)) \
        .select((col("row") - half_size).alias("row"), col("col"), col("value"))
    
    # Bottom-right (A22)
    a22 = df.filter((col("row") >= half_size) & (col("col") >= half_size)) \
        .select((col("row") - half_size).alias("row"), (col("col") - half_size).alias("col"), col("value"))
    
    return a11, a12, a21, a22


def merge_quadrants(c11, c12, c21, c22, half_size):
    """Merge 4 quadrants back into a single matrix"""
    # Adjust coordinates for each quadrant
    c11_adj = c11
    c12_adj = c12.select(col("row"), (col("col") + half_size).alias("col"), col("value"))
    c21_adj = c21.select((col("row") + half_size).alias("row"), col("col"), col("value"))
    c22_adj = c22.select((col("row") + half_size).alias("row"), (col("col") + half_size).alias("col"), col("value"))
    
    # Union all quadrants
    result = c11_adj.union(c12_adj).union(c21_adj).union(c22_adj)
    return result


def strassen_multiply(df_a, df_b, size, depth=0):
    """
    Strassen matrix multiplication algorithm
    Uses divide-and-conquer with 7 recursive multiplications instead of 8
    """
    indent = "  " * depth
    print(f"{indent}Strassen: size={size}, depth={depth}")
    
    # Base case: use standard multiplication for small matrices OR max depth reached
    if size <= STRASSEN_THRESHOLD or depth >= MAX_DEPTH:
        if depth >= MAX_DEPTH:
            print(f"{indent}Max depth reached, using standard multiplication")
        else:
            print(f"{indent}Using standard multiplication (size={size})")
        return standard_multiply(df_a, df_b, size)
    
    # Split matrices into quadrants
    print(f"{indent}Splitting matrices...")
    a11, a12, a21, a22 = split_matrix(df_a, size)
    b11, b12, b21, b22 = split_matrix(df_b, size)
    
    half_size = size // 2
    
    # Cache the split matrices to avoid recomputation
    for df in [a11, a12, a21, a22, b11, b12, b21, b22]:
        df.cache()
    
    # Force materialization of cached data to prevent lazy evaluation issues
    for df in [a11, a12, a21, a22, b11, b12, b21, b22]:
        df.count()
    
    # Compute the 7 Strassen products
    print(f"{indent}Computing M1...")
    m1 = strassen_multiply(
        add_matrices(a11, a22),
        add_matrices(b11, b22),
        half_size, depth + 1
    )
    m1.cache()
    m1.count()  # Materialize
    
    print(f"{indent}Computing M2...")
    m2 = strassen_multiply(
        add_matrices(a21, a22),
        b11,
        half_size, depth + 1
    )
    m2.cache()
    m2.count()
    
    print(f"{indent}Computing M3...")
    m3 = strassen_multiply(
        a11,
        subtract_matrices(b12, b22),
        half_size, depth + 1
    )
    m3.cache()
    m3.count()
    
    print(f"{indent}Computing M4...")
    m4 = strassen_multiply(
        a22,
        subtract_matrices(b21, b11),
        half_size, depth + 1
    )
    m4.cache()
    m4.count()
    
    print(f"{indent}Computing M5...")
    m5 = strassen_multiply(
        add_matrices(a11, a12),
        b22,
        half_size, depth + 1
    )
    m5.cache()
    m5.count()
    
    print(f"{indent}Computing M6...")
    m6 = strassen_multiply(
        subtract_matrices(a21, a11),
        add_matrices(b11, b12),
        half_size, depth + 1
    )
    m6.cache()
    m6.count()
    
    print(f"{indent}Computing M7...")
    m7 = strassen_multiply(
        subtract_matrices(a12, a22),
        add_matrices(b21, b22),
        half_size, depth + 1
    )
    m7.cache()
    m7.count()
    
    # Combine results to get final quadrants
    print(f"{indent}Combining results...")
    c11 = add_matrices(subtract_matrices(add_matrices(m1, m4), m5), m7)
    c12 = add_matrices(m3, m5)
    c21 = add_matrices(m2, m4)
    c22 = add_matrices(subtract_matrices(add_matrices(m1, m3), m2), m6)
    
    # Merge quadrants
    result = merge_quadrants(c11, c12, c21, c22, half_size)
    
    # Unpersist intermediate results to free memory and close connections
    for df in [a11, a12, a21, a22, b11, b12, b21, b22, m1, m2, m3, m4, m5, m6, m7]:
        df.unpersist()
    
    return result


# Main execution

results_log = []
start_time = datetime.now()

for n in range(10, 16, 1):
    matrix_size = 2 ** n
    input_path = f"{HADOOP_PATH}/matrices_{n}"
    output_path = f"{RESULTS_PATH}/matrices_{n}"
    
    
    iter_start = time.time()
    
    try:
        # Load matrices
        print("[1/3] Loading matrices...")
        start_load = time.time()
        df_matrix1 = spark.read.parquet(f"{input_path}/matrix1.parquet")
        df_matrix2 = spark.read.parquet(f"{input_path}/matrix2.parquet")
        load_time = time.time() - start_load
                
        # Cache input matrices
        df_matrix1.cache()
        df_matrix2.cache()
        
        # Strassen multiplication
        print("[2/3] Computing Strassen multiplication...")
        start_compute = time.time()
        df_result = strassen_multiply(df_matrix1, df_matrix2, matrix_size)
        
        # Write result
        print("[3/3] Writing result to HDFS...")
        start_write = time.time()
        df_result.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(f"{output_path}/strassen_result_{n}")
        write_time = time.time() - start_write
        
        compute_time = time.time() - start_compute
        total_time = time.time() - iter_start
                
        print(f"{'=' * 80}")
        print(f"✓ SUCCESS - n={n}")
        print(f"  Load time:    {load_time:8.2f}s")
        print(f"  Compute time: {compute_time:8.2f}s")
        print(f"  Write time:   {write_time:8.2f}s")
        print(f"  Total time:   {total_time:8.2f}s")
        print(f"{'=' * 80}")
        
        # Store timing information
        timing_info = {
            "order": n,
            "matrix_size": matrix_size,
            "algorithm": "strassen",
            "threshold": STRASSEN_THRESHOLD,
            "load_time_seconds": round(load_time, 2),
            "compute_time_seconds": round(compute_time, 2),
            "write_time_seconds": round(write_time, 2),
            "total_time_seconds": round(total_time, 2),
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        results_log.append(timing_info)
        
        # Save timing for this iteration
        timing_df = spark.createDataFrame([timing_info])
        timing_df.write \
            .mode("overwrite") \
            .json(f"{output_path}/timing_{n}")
        
        print(f"--->✓ Timing data saved\n\n")
        
        # Clear cache
        df_matrix1.unpersist()
        df_matrix2.unpersist()
        spark.catalog.clearCache()
        
    except Exception as e:
        error_time = time.time() - iter_start
        print(f"\n{'=' * 80}")
        print(f"✗ ERROR - n={n}")
        print(f"Error: {str(e)}")
        print(f"Time before error: {error_time:.2f}s")
        print(f"{'=' * 80}\n")
        
        import traceback
        traceback.print_exc()
        
        results_log.append({
            "order": n,
            "matrix_size": matrix_size,
            "error": str(e),
            "time_before_error": round(error_time, 2),
            "timestamp": datetime.now().isoformat(),
            "status": "failed"
        })
        
        # Clear cache and continue
        spark.catalog.clearCache()

spark.stop()
print("\n✓ Spark session stopped.")