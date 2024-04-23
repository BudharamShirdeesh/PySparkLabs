from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Iris").getOrCreate()

#iris dataset
iris_df = spark.read.csv("iris.csv", header=True)

# Count of each variety
count_by_variety = iris_df.groupBy("variety").count()
count_by_variety.show()

