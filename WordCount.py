from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

logFile = "hamlet.txt"  # Should be some file on your system

spark = SparkSession.builder.appName("WordCount").getOrCreate()

logData = spark.read.text(logFile).cache()

# Create a new dataframe by splitting each row from logData. In effect,
# the new df splitWords will have one col, wordLists, with each element
# being a list of the words in each line of Hamlet.
splitWords = logData.select(sf.split(logData.value, ' ').alias('wordLists'))
# Explode over the lists of words, creating a col where each elt is a single word.
# Take count of this col to get word count.
numWords = splitWords.select( sf.explode(splitWords.wordLists) ).count()

print("Number of Words: %i" % (numWords))

spark.stop()