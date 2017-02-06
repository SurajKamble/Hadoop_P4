# Spark example to print the average tweet length using Spark
# PGT April 2016
# To run, do: spark-submit --master yarn-client avgTweetLength.py hdfs://hadoop2-0-0/data/1gram/googlebooks-eng-all-1gram-20120701-x

from pyspark import SparkContext
import sys
import matplotlib.pyplot as plt

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("enter a filename")
        sys.exit(1)

    sc = SparkContext(appName="AvgWordLength")

    one_gram_file = sc.textFile(sys.argv[1], )
    row = one_gram_file.map(lambda line: (line.split('\t')[1], [len(line.split('\t')[0]), 1])).reduceByKey(lambda a, b:
                                                                                        ((a[0]+b[0]), (a[1]+b[1])))
    count = row.reduceByKey(lambda a, b: (a[0]+b[0])/(a[1]+b[1]))

    '''
        one_gram_file.flatMap(
        lambda line: (line.split('\t')[1], len(line.split('\t')[0]))).map(
        lambda col: (col[0], [col[1], 1])).reduceByKey(
        lambda a, b: (a[0] + b[0]) / (a[1] + b[1]))
    '''

    row.saveAsTextFile("AvgWC")
    sc.stop()
    x=[]
    y=[]
    for c in count.take(10):
        x.append(c[0])
        y.append(c[1])

    plt.plot(x, y, 'o')
    print count.stats()
    plt.show()

    # hdfs://hadoop2-0-0/data/1gram/googlebooks-eng-all-1gram-20120701-z

    # one_gram.map(lambda line: (line.split('\t')[1], [len(line.split('\t')[0]), 1])).reduceByKey(lambda a,b:
            # (a[0]+b[0])/(a[1]+b[1]))
