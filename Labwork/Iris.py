from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, stddev_pop, mean, col

# Create a SparkSession
spark = SparkSession.builder.appName("IrisAnalysis").getOrCreate()

# Read iris dataset
iris_df = spark.read.csv("iris.csv", header=True)

# Simplify column names
avg_iris_df = iris_df.selectExpr(
    "variety",
    "`sepal.length` AS sepal_length",
    "`sepal.width` AS sepal_width",
    "`petal.length` AS petal_length",
    "`petal.width` AS petal_width"
)

# Find distinct varieties
varieties = avg_iris_df.select("variety").distinct().rdd.flatMap(lambda x: x).collect()

# 1. Count of each variety
count_by_variety = iris_df.groupBy("variety").count()
print("1. Count of each variety:")
count_by_variety.show()

# 2. Find the average of attributes based on the variety
print("2. Average of attributes based on the variety:")
for variety in varieties:
    variety_df = avg_iris_df.filter(col("variety") == variety)
    avg_variety = variety_df.groupBy("variety").agg(
        avg("sepal_length").alias("avg_sepal_length"),
        avg("sepal_width").alias("avg_sepal_width"),
        avg("petal_length").alias("avg_petal_length"),
        avg("petal_width").alias("avg_petal_width")
    )
    print(f"Variety: {variety}")
    avg_variety.show()

# 3. Find the difference between the average of attributes based on variety
print("3. Difference between the averages based on variety:")
difference = {}
for i in range(len(varieties)):
    for j in range(i+1, len(varieties)):
        variety1 = varieties[i]
        variety2 = varieties[j]
        diff_key = f"{variety1} - {variety2}"
        
        avg_variety1 = avg_iris_df.filter(col("variety") == variety1).groupBy("variety").agg(
            avg("sepal_length").alias("avg_sepal_length"),
            avg("sepal_width").alias("avg_sepal_width"),
            avg("petal_length").alias("avg_petal_length"),
            avg("petal_width").alias("avg_petal_width")
        ).first()
        
        avg_variety2 = avg_iris_df.filter(col("variety") == variety2).groupBy("variety").agg(
            avg("sepal_length").alias("avg_sepal_length"),
            avg("sepal_width").alias("avg_sepal_width"),
            avg("petal_length").alias("avg_petal_length"),
            avg("petal_width").alias("avg_petal_width")
        ).first()
        
        diff_value = {
            "sepal.length": avg_variety1["avg_sepal_length"] - avg_variety2["avg_sepal_length"],
            "sepal.width": avg_variety1["avg_sepal_width"] - avg_variety2["avg_sepal_width"],
            "petal.length": avg_variety1["avg_petal_length"] - avg_variety2["avg_petal_length"],
            "petal.width": avg_variety1["avg_petal_width"] - avg_variety2["avg_petal_width"]
        }
        
        difference[diff_key] = diff_value
        print(f"Difference between {variety1} and {variety2}:")
        print(diff_value)

# 4. Find the outliers for each variety
print("4. Outliers for each variety:")
mean_std_df = iris_df.groupBy("variety").agg(
    mean("`sepal.length`").alias("mean_sepal_length"),
    mean("`sepal.width`").alias("mean_sepal_width"),
    mean("`petal.length`").alias("mean_petal_length"),
    mean("`petal.width`").alias("mean_petal_width"),
    stddev_pop("`sepal.length`").alias("stddev_sepal_length"),
    stddev_pop("`sepal.width`").alias("stddev_sepal_width"),
    stddev_pop("`petal.length`").alias("stddev_petal_length"),
    stddev_pop("`petal.width`").alias("stddev_petal_width")
)

outliers_list = []
for variety in mean_std_df.collect():
    variety_name = variety["variety"]
    sepal_length_upper_bound = variety["mean_sepal_length"] + 2 * variety["stddev_sepal_length"]
    sepal_length_lower_bound = variety["mean_sepal_length"] - 2 * variety["stddev_sepal_length"]
    sepal_width_upper_bound = variety["mean_sepal_width"] + 2 * variety["stddev_sepal_width"]
    sepal_width_lower_bound = variety["mean_sepal_width"] - 2 * variety["stddev_sepal_width"]
    petal_length_upper_bound = variety["mean_petal_length"] + 2 * variety["stddev_petal_length"]
    petal_length_lower_bound = variety["mean_petal_length"] - 2 * variety["stddev_petal_length"]
    petal_width_upper_bound = variety["mean_petal_width"] + 2 * variety["stddev_petal_width"]
    petal_width_lower_bound = variety["mean_petal_width"] - 2 * variety["stddev_petal_width"]
    
    outliers_df = iris_df.filter(
        (iris_df["variety"] == variety_name) &
        (
            (iris_df["`sepal.length`"] > sepal_length_upper_bound) |
            (iris_df["`sepal.length`"] < sepal_length_lower_bound) |
            (iris_df["`sepal.width`"] > sepal_width_upper_bound) |
            (iris_df["`sepal.width`"] < sepal_width_lower_bound) |
            (iris_df["`petal.length`"] > petal_length_upper_bound) |
            (iris_df["`petal.length`"] < petal_length_lower_bound) |
            (iris_df["`petal.width`"] > petal_width_upper_bound) |
            (iris_df["`petal.width`"] < petal_width_lower_bound)
        )
    )
    outliers_list.append(outliers_df)

for variety, outliers_df in zip(mean_std_df.collect(), outliers_list):
    print(f"Outliers for variety '{variety['variety']}':")
    outliers_df.show()
