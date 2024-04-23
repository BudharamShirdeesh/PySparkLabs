from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev_pop, mean, col

# Create a SparkSession
spark = SparkSession.builder.appName("IrisAnalysis").getOrCreate()

# Load the iris dataset
iris_df = spark.read.csv("iris.csv", header=True)

# Find mean and standard deviation for each column
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


# Calculate upper and lower bounds for each variety
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

# Display outliers for each variety
for variety, outliers_df in zip(mean_std_df.collect(), outliers_list):
    variety_name = variety["variety"]
    print(f"Outliers for variety '{variety_name}':")
    outliers_df.show()

