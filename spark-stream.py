from pyspark import SparkContext
from pyspark.streaming  import StreamingContext
from pyspark.sql import functions as f

if __name__ == '__main__':
    sc = SparkContext("local[3]", "SparkStream")
    ssc = StreamingContext(sc, 1)
    lines = ssc.socketTextStream("localhost",9999)
    words = lines.flatMap(lambda line : line.split(" "))
    pairs = words.map(lambda word : (word, 1))
    word_count = pairs.reduceByKey(lambda prev,  acc:  prev + acc)
    word_count.pprint()
    ssc.start()
    input("Enter to exit")