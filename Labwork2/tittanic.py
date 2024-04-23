from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, avg, col, when, count, sum,floor

spark = SparkSession.builder.appName("Titanic Analysis").getOrCreate()
df = spark.read.csv("Titanic-Dataset.csv", header=True, inferSchema=True)

## 1. Calculate the mean fare
mean_fare = df.select(mean(df['Fare'])).collect()[0][0]
print("1. Mean ticket fare:", mean_fare)

## 2. Calculate the six-point summary of age for survived passengers
survived_df = df.filter(col("Survived") == 1)
not_survived_df = df.filter(col("Survived") == 0)

survived_age_summary = survived_df.select("Age").summary("min", "25%", "50%", "75%", "max", "count")
not_survived_age_summary = not_survived_df.select("Age").summary("min", "25%", "50%", "75%", "max", "count")

print("2. Six-point summary of age for survived passengers:")
survived_age_summary.show()
print("2. Six-point summary of age for passengers who did not survive:")
not_survived_age_summary.show()

## 3. Calculate survival rate based on the number of siblings
survival_rate_by_siblings = df.groupBy("SibSp").agg(avg("Survived").alias("SurvivalRate"))
print("3. Survival rate based on the number of siblings:")
survival_rate_by_siblings.show()

## 4. Calculate survival probability based on gender
survival_probability_by_gender = df.groupBy("Sex").agg(avg("Survived").alias("SurvivalProbability"))
print("4. Survival probability based on gender:")
survival_probability_by_gender.show()

## 5. Calculate survival rate by age group
new_df = df.withColumn("age_group", floor(col("age") / 10) * 10)
survival_rates = new_df.groupBy("age_group").agg(
    (sum(when(col("survived") == 1, 1).otherwise(0)) / count("*")).alias("survival_rate")
).orderBy("age_group")
max_group = survival_rates.orderBy(col("survival_rate").desc()).first()
print("5.value:", max_group[0], " age group with survival rate of ", max_group[1])

## 6. Calculate average survival rate based on the Embarked City
average_survival_rate_by_embarked = df.groupBy("Embarked").agg(avg("Survived").alias("AverageSurvivalRate"))
print("6. Average survival rate based on the Embarked City:")
average_survival_rate_by_embarked.show()

##8.
# Add AgeGroup column
df_with_age_group = df.withColumn("AgeGroup", 
                                  when(col("Age") < 10, "0-9")
                                  .when((col("Age") >= 10) & (col("Age") < 20), "10-19")
                                  .when((col("Age") >= 20) & (col("Age") < 30), "20-29")
                                  .when((col("Age") >= 30) & (col("Age") < 40), "30-39")
                                  .when((col("Age") >= 40) & (col("Age") < 50), "40-49")
                                  .when((col("Age") >= 50) & (col("Age") < 60), "50-59")
                                  .when((col("Age") >= 60) & (col("Age") < 70), "60-69")
                                  .otherwise("70+"))

# Group the data by age group, gender, class, and boarding city
passenger_group_survival_rate = df_with_age_group.groupBy("AgeGroup", "Sex", "Pclass", "Embarked").agg(
    avg("Survived").alias("SurvivalRate")
)

# Find the passenger group with the highest survival rate
max_survival_group = passenger_group_survival_rate.orderBy(col("SurvivalRate").desc()).first()
print(" 8. Passenger group with the highest survival rate:")
print("AgeGroup:", max_survival_group["AgeGroup"])
print("Gender:", max_survival_group["Sex"])
print("Class:", max_survival_group["Pclass"])
print("Boarding City:", max_survival_group["Embarked"])
print("Survival Rate:", max_survival_group["SurvivalRate"])

# Find the passenger group with the least survival rate
min_survival_group = passenger_group_survival_rate.orderBy(col("SurvivalRate")).first()
print("\n 8.Passenger group with the least survival rate:")
print("AgeGroup:", min_survival_group["AgeGroup"])
print("Gender:", min_survival_group["Sex"])
print("Class:", min_survival_group["Pclass"])
print("Boarding City:", min_survival_group["Embarked"])
print("Survival Rate:", min_survival_group["SurvivalRate"])

spark.stop()