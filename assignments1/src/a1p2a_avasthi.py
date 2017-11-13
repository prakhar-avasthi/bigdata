from pyspark import SparkContext
from random import random


class Solution:

    def __init__(self):
        self.sc = SparkContext("local", "a1p2a")
        self.sc.setLogLevel("FATAL")


    #########################################################################
    ################ WordCount implementation below #########################
    #########################################################################

    def count(self, data):
        # 1. Map transform input from [(1, "sentence number one"),(2,"sentence number two")]
        #  => ["sentence number one", "sentence number two"]
        # 2. FlatMap transform will split words => ["sentence","number","one","sentence","number","two"]
        # 3. Map transform to add 1 to each word as key
        # => [("sentence",1),("number",1),("one",1),("sentence",1),("number",1),("two",1)]
        # 4. reduceByKey transform to add value of same key => [("sentence",2),("number",2),("one",1),("two",1)]
        rdd = self.sc.parallelize(data)
        words = rdd.map(lambda a: a[1])\
            .flatMap(lambda a: a.split(" "))\
            .map(lambda a: (a.lower(), 1))\
            .reduceByKey(lambda a, b: a+b)
        print(words.collect())

        #########################################################################
        ################ SetDifference implementation below #####################
        #########################################################################

    def setDifference(self, data):
        # 1. FlatMap transform input to [(v1,R),(v2,R),(v3,R),(v4,R),(w1,S),(v2,S),(v4,S)]
        # 2. Map transform to change R to 1 and S to -1 => [(v1,1),(v2,1),(v3,1),(v4,1),(w1,-1),(v2,-1),(v4,-1)]
        # 3. reduceByMap transform to add values with same key => [(v1,1),(v2,0),(v3,1),(v4,0),(w1,-1)]
        # 4. flatMap transform to remove key with value 0 or less than 0 => [[v1],[],[v3],[],[]] => [v1, v3]
        rdd = self.sc.parallelize(data)
        val = rdd.flatMap(lambda a: [(c,a[0]) for c in a[1]]).\
            map(lambda a: (a[0], 1) if a[1] == 'R' else (a[0], -1)).\
            reduceByKey(lambda a, b: a + b)\
            .flatMap(lambda a: [a[0] for i in range(a[1])])
        print(val.collect())


if __name__ == "__main__":  # [DONE: Uncomment peices to test]

    ####################
    ## run WordCount: ##
    ####################
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8,
             "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
            (10, "Car engines purred and the tires burned.")]
    mrObject = Solution()
    mrObject.count(data)

    #######################
    ## run SetDifference ##
    #######################
    print ("\n\n*****************\n Set Difference\n*****************\n")
    data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']),
    		 ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
    mrObject.setDifference(data1)

    data2 = [('R', [x for x in range(50) if random() > 0.5]),
    		 ('S', [x for x in range(50) if random() > 0.75])]
    mrObject.setDifference(data2)