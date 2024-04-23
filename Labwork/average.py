from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Create a SparkSession
spark = SparkSession.builder.appName("IrisAnalysis").getOrCreate()

#iris dataset
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

# Iterate over each variety
for variety in varieties:
    # Filter DataFrame for the current variety
    variety_df = avg_iris_df.filter(col("variety") == variety)
    
    # Calculate average based on variety
    avg_variety = variety_df.groupBy("variety").agg(
        avg("sepal_length").alias("avg_sepal_length"),
        avg("sepal_width").alias("avg_sepal_width"),
        avg("petal_length").alias("avg_petal_length"),
        avg("petal_width").alias("avg_petal_width")
    )
    
    # Display the result
    print(f"Variety: {variety}")
    avg_variety.show()

