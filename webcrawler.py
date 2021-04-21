from pyspark.sql import SQLContext
from pyspark import SparkContext
import pyspark.sql.functions as F
import re
import requests

base_url = "https://hari1500.github.io/CS387-lab7-crawler-website"
already_scraped = dict()

def scrape_page(url):
    url1 = base_url + url
    print(url1)
    
    url_list = []
    h = requests.head(url1)
    print("Head get done")
    header = h.headers
    content_type = header.get('content-type')

    if(content_type=='text/html; charset=utf-8'):
        h = requests.get(url1)
        print("URL get done")
        body = h.text
        links = re.findall(r"<a href=\".*\">.*</a>",body)
        for link in links:
            url_list.append("/"+(link.split("href=")[1].split(">")[0])[1:-1])
    
    return url_list

if __name__ == "__main__":

    spark = SparkContext("local","Webcrawler")
    orig_file = ["/1.html"]

    rdd = spark.parallelize(orig_file)
    rdd_updated = rdd

    while(1):
        links = rdd_updated.flatMap(scrape_page)
        rdd_new = spark.union([links,rdd])
        if rdd_new.count() > rdd.count():
            rdd_updated =rdd_new.subtract(rdd).distinct()
            rdd = rdd_new
        else:
            break

    in_degree = rdd_new.map(lambda x:(x,1)).reduceByKey(lambda v1,v2:v1+v2)
    in_degree.saveAsTextFile("./webcrawler/")