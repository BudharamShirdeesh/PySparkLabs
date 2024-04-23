from pyspark.sql import SparkSession
from random import randint as rand
def wordcount_mapper(word):
    return (word,1)
def wordcount_reducer(a,b):
    return(a+b)
def splitter(line):
    return line.split(" ")
def remove_stopwords(word_count_list):
    stopwords = set(['the', 'and', 'is', 'in', 'it', 'of', 'to','that'])
    return [(word, count) for word, count in word_count_list if word.lower() not in stopwords]

logFile = "input.txt"
spark = SparkSession.builder.appName("WordCount").master("local").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
read_file = spark.sparkContext.textFile(logFile)
words = read_file.flatMap(splitter)
word_count_mapped = words.map(wordcount_mapper)
word_count_reduced = word_count_mapped.reduceByKey(wordcount_reducer)
temp = word_count_reduced.collect()
filtered_temp = remove_stopwords(temp)
for x in filtered_temp:
    print(x)
spark.stop()

