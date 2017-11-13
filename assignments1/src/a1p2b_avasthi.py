from pyspark import SparkContext
import re


class Solution:

    def __init__(self, blog_path):
        self.sc = SparkContext("local", "a1p2b")
        self.sc.setLogLevel("ERROR")
        self.blog_path = blog_path
        self.filesRdd = self.sc.wholeTextFiles(self.blog_path, None, False)
        self.ind_list = None

    def get_industry_name(self):
        # 1. Map transform Filename from FileRDD => [(dir/file1),(dir/file2)]
        # 2. Replace directory path and get industry name by splitting on basis of "."
        # 3. ReduceByKey transform to get unique Industry name
        # 4. save list in broadcast variable
        print("############################ Get Industry Name ###############################")
        blog_files = self.filesRdd \
            .map(lambda a: (a[0].split(".")[3].lower(), 1))\
            .reduceByKey(lambda a, b: a + b)\
            .map(lambda a: a[0].lower())

        self.ind_list = self.sc.broadcast([x for x in blog_files.toLocalIterator()])
        print(self.ind_list.value)
        print("##############################################################################")

    def get_industry_mention(self):
        # 1. FlatMap transform to get content from FileRDD => [(file_1_content),(file_2_content)] and remove whitespaces
        # 2. FlatMap transform to apply regex to fetch date and post combination => [("date1", "post1"),("date2", "post2")]
        # 3. FlatMap transform to get (date, word) combination for all posts => [(date1, w1),(date1,w2),(date2,v1),(date2,v2)]
        # 4. FlatMap transform to check is word in industry list => [((d1, ind1),1),((d1, ind1),1),((d1, ind2),1),((d2, ind1),1)]
        # 5. ReduceByKey transform to collect count of date, word and industry combination => [((d1, ind1),2),((d1, ind2),1),((d2, ind1),1)]
        # 6. FlatMap is word count is greater than 0
        # 7. ReduceByKey to get combination of (Industry, [(Date1, count1), (date2, count2)]
        print("############################ Get Industry Mention ###########################")
        ind_list = self.ind_list
        blog_content = self.filesRdd\
            .map(lambda a: [a[1].replace("\r\n", " ")])\
            .flatMap(lambda a: re.findall("<date>.*?</post>", a[0]))\
            .flatMap(lambda a: [a.split("</date> <post>")])\
            .map(lambda a: [a[0].replace("<date>", ""), a[1].replace("</post>", "").split(" ")])\
            .flatMap(lambda a: [((a[0].split(",")[2]+"-"+a[0].split(",")[1]), re.sub("[.]$", "", b.lower())) for b in a[1]])\
            .flatMap(lambda a: [((a[0], a[1]), 1) if a[1] in ind_list.value else ((a[0], a[1]), 0)]) \
            .reduceByKey(lambda a, b: a + b)\
            .flatMap(lambda a: [(a[0], a[1])] if a[1] > 0 else [])\
            .flatMap(lambda a: [(a[0][1], [(a[0][0], a[1])])]) \
            .reduceByKey(lambda a, b: a + b)
        print([x for x in blog_content.toLocalIterator()])
        print("##############################################################################")


if __name__ == "__main__":
    blog_path = "/home/prakhar/Study/3_ms/fall_2017/big_data_system/assignments/assignments1/test"

    obj = Solution(blog_path)
    obj.get_industry_name()
    obj.get_industry_mention()
