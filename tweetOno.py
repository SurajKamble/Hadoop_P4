# Spark program to print the average tweet length using Spark



from __future__ import print_function
import sys, json
from pyspark import SparkContext


def getText(line):
  try:
    js = json.loads(line)
    if js['user']['screen_name']=='PrezOno':
      text = js['text'].encode('ascii', 'ignore')
      return ['Prez_Ono',[len(text),1]]
    else:
      text = js['text'].encode('ascii', 'ignore')
      return ['other',[len(text),1]]
  except Exception as a:
    return []
  
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
  sc = SparkContext(appName="PrezOnoavgtweetlength")
  tweets = sc.textFile(sys.argv[1],)
  texts = tweets.map(getText)

  tweetcount = texts.map(lambda a: (a[0],a[1][1])).reduceByKey(lambda a,b: (a+b))
  print (tweetcount.take(5))
  tweetlength = texts.map(lambda a: (a[0],a[1][0])).reduceByKey(lambda a,b: (a+b))
  print  ('others average\t',tweetlength.take(5)[1][1]/tweetcount.take(5)[1][1])
  print  ('PrezOno average\t',tweetlength.take(5)[0][1]/tweetcount.take(5)[0][1])
  lengths.saveAsTextFile("Qsn3_lengths_twit3")
  sc.stop()


