
from pyspark.sql import SQLContext
from pyspark import SparkContext
import re
# other required imports here

# +
#line = 'aaaaAAA@@@ff%$4#4'
# -

#


l = ['1','2','3','4']
" ".join(l[:-1])



def process(line):
    # fill
    # convert all characters into lower case
    # replace all non-alphanumerics with whitespace
    # split on whitespaces
    # return list of words
    #print(type(line))
    line_str = line.asDict()["article_body"]
    line_str = line_str.lower()
    
    line_date = line.asDict()["date_published"]
    line_date = line_date.split('-')[:-1]
    line_dates = '-'.join(line_date)
    
    
    
    line_str = re.sub("[^0-9a-zA-Z]+", " ", line_str)
    l = line_str.split()
    l = [line_dates + ' ' + i for i in l]
    
    return l

# +
#spark = SparkContext("local", "Word Count")

# read json data from the newdata directory
#df = SQLContext(spark).read.option("multiLine", True).option("mode", "PERMISSIVE").json("./newsdata")

    # split each line into words
#lines = df.select("date_published", "article_body").rdd
#print(lines[0])
#words = lines.flatMap(process)
#print(type(words))
# -



if __name__ == "__main__":
    # create Spark context with necessary configuration
    spark = SparkContext("local", "Word Count")

    # read json data from the newdata directory
    df = SQLContext(spark).read.option("multiLine", True) \
    .option("mode", "PERMISSIVE").json("./newsdata")

    # split each line into words
    lines = df.select("date_published", "article_body").rdd
    words = lines.flatMap(process)
    
    #print(type(words))
    
    # count the occurrence of each word
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1+v2)

    # save the counts to output
    wordCounts.saveAsTextFile("./wordcount/")
