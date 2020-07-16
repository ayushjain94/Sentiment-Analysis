from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)  # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")

    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    print(counts)
    positive_figures = []
    negative_figures = []
    for count in counts:
        if len(count) > 1:
            positive_figures.append(count[0][1])
            negative_figures.append(count[1][1])

    plt.plot(positive_figures, '-o', color="#333333", label='Positive')
    plt.plot(negative_figures, '-o', color="green", label='Negative')
    plt.xlabel("Time step")
    plt.ylabel("Word count")
    plt.legend(loc='upper left')
    plt.show()

def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    wordlist = open(filename).read().strip().split()
    return wordlist


def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics=['twitterstream'], kafkaParams={"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    tweets = tweets.flatMap(lambda x: x.split(' ')).map(lambda word: word.lower())
    tweets = tweets.map(lambda x: ('positive', 1) if x in pwords else ('negative', 1) if x in nwords else ('none', 1))
    tweets = tweets.filter(lambda word: word[0] == 'positive' or word[0] == 'negative')
    positive_negative_count = tweets.reduceByKey(lambda positive, negative: positive + negative)
    state_wise_count = positive_negative_count.updateStateByKey(updateFunction)
    state_wise_count.pprint()
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    positive_negative_count.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    ssc.start()  # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__ == "__main__":
    main()
