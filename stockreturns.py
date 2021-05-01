
from pyspark.sql import SQLContext
from pyspark import SparkContext
import pyspark.sql.functions as F
import re
# other required imports here

# +
#line = 'aaaaAAA@@@ff%$4#4'
# -

#


if __name__ == "__main__":
    # create Spark context with necessary configuration
  
    spark = SparkContext("local", "Word Count")

    # read json data from the newdata directory
    #df = SQLContext(spark).read.option("multiLine", True) \
    #.option("mode", "PERMISSIVE").csv("stock_prices.csv")
    df = SQLContext(spark).read.load("stock_prices.csv",format="csv", sep=",", inferSchema="true", header="true")
    
    df.show()
    # split each line into words
    #lines = df.select("open", "close", "date")
    #words = lines.flatMap(process)
    #print(lines)
    #lines = df.select("open", "close", "date")
    lines_percent = df.withColumn("percent", 100.0*(F.col("close")-F.col("open"))/(F.col("open")*1.0))
    lines_percent.show()

    t = lines_percent.groupBy(["date"]).agg(F.sum("percent").alias("total_return"))
    df_ = df.join(t, ["date"], how = 'left')
    t = df_.groupBy(["date"]).agg(F.count("ticker").alias("number of tickers"))
    df__ = df_.join(t, ["date"], how = 'left')
    df__.show(1000)

    lines_percent = df__.withColumn("avg_return", F.col("total_return")/F.col("number of tickers"))
    final = lines_percent.select("date", "avg_return")
    final.count()                                                   

    fi = final.groupBy(["date", "avg_return"]).agg(F.count("date").alias("cnt"))
    fii = fi.select("date", "avg_return")
    fii.count()
    fii.write.csv('./stockreturns/')

    #fii.rdd.map(tuple).saveAsTextFile("./stockreturns/")    

    #print(type(words))
    
    # count the occurrence of each word
    #wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1+v2)

    # save the counts to output
    #wordCounts.saveAsTextFile("./wordcount/")
