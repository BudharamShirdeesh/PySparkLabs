from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.appName("IrisAnalysis").getOrCreate()

# Load the iris dataset
iris_df = spark.read.csv("iris.csv", header=True)

# Filter DataFrames for each variety
setosa_df = iris_df.filter(col("variety") == "Setosa")
versicolor_df = iris_df.filter(col("variety") == "Versicolor")
virginica_df = iris_df.filter(col("variety") == "Virginica")

# Calculate the average for each variety
# Setosa
avg_setosa = setosa_df.selectExpr(
    "avg(`sepal.length`) as avg_sepal_length_setosa",
    "avg(`sepal.width`) as avg_sepal_width_setosa",
    "avg(`petal.length`) as avg_petal_length_setosa",
    "avg(`petal.width`) as avg_petal_width_setosa"
).first()

# Versicolor
avg_versicolor = versicolor_df.selectExpr(
    "avg(`sepal.length`) as avg_sepal_length_versicolor",
    "avg(`sepal.width`) as avg_sepal_width_versicolor",
    "avg(`petal.length`) as avg_petal_length_versicolor",
    "avg(`petal.width`) as avg_petal_width_versicolor"
).first()

# Virginica
avg_virginica = virginica_df.selectExpr(
    "avg(`sepal.length`) as avg_sepal_length_virginica",
    "avg(`sepal.width`) as avg_sepal_width_virginica",
    "avg(`petal.length`) as avg_petal_length_virginica",
    "avg(`petal.width`) as avg_petal_width_virginica"
).first()

# Calculate the differences between the averages
difference = {
    "Setosa - Versicolor": {
        "sepal.length": avg_setosa["avg_sepal_length_setosa"] - avg_versicolor["avg_sepal_length_versicolor"],
        "sepal.width": avg_setosa["avg_sepal_width_setosa"] - avg_versicolor["avg_sepal_width_versicolor"],
        "petal.length": avg_setosa["avg_petal_length_setosa"] - avg_versicolor["avg_petal_length_versicolor"],
        "petal.width": avg_setosa["avg_petal_width_setosa"] - avg_versicolor["avg_petal_width_versicolor"]
    },
    "Virginica - Setosa": {
        "sepal.length": avg_virginica["avg_sepal_length_virginica"] - avg_setosa["avg_sepal_length_setosa"],
        "sepal.width": avg_virginica["avg_sepal_width_virginica"] - avg_setosa["avg_sepal_width_setosa"],
        "petal.length": avg_virginica["avg_petal_length_virginica"] - avg_setosa["avg_petal_length_setosa"],
        "petal.width": avg_virginica["avg_petal_width_virginica"] - avg_setosa["avg_petal_width_setosa"]
    },
    "Virginica - Versicolor": {
        "sepal.length": avg_virginica["avg_sepal_length_virginica"] - avg_versicolor["avg_sepal_length_versicolor"],
        "sepal.width": avg_virginica["avg_sepal_width_virginica"] - avg_versicolor["avg_sepal_width_versicolor"],
        "petal.length": avg_virginica["avg_petal_length_virginica"] - avg_versicolor["avg_petal_length_versicolor"],
        "petal.width": avg_virginica["avg_petal_width_virginica"] - avg_versicolor["avg_petal_width_versicolor"]
    }
}

# Print the differences
print("Difference between Setosa and Versicolor:")
print(difference["Setosa - Versicolor"])
print("\nDifference between Virginica and Setosa:")
print(difference["Virginica - Setosa"])
print("\nDifference between Virginica and Versicolor:")
print(difference["Virginica - Versicolor"])

