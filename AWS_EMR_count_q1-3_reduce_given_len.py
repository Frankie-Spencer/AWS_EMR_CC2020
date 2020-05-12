import sys
from pyspark import SparkContext, HiveContext

sc = SparkContext()
sqlContext = HiveContext(sc)

# system input argument needed
# e. g /dir/text.txt /dir/out 3-5
args = sys.argv
source_path, out_path, len_word = args[1], args[2], args[3]

# declare removable items
# -empty strings and which could be created when splitting
# -None values as created if the word doesn't include alphabet
remove_items = sc.parallelize([(None, None)])

# import input file, and set use_unicode to True to recognise Finnish letters
input_file = sc.textFile(source_path, use_unicode=True)


# this function will validate the word whether it contains at least an alphabet
# and the check if the word length is within given range
def word_validate(w):

    if any(s.isalpha() for s in w):
            try:
                len_f_t = len_word.split('-')
                f = int(len_f_t[0].strip())
                t = int(len_f_t[1].strip())
                if f <= len(w) <= t:
                    words_amount_key = 'words_of_chars_' + str(f) + '-' + str(t)
                    return words_amount_key
                else:
                    return None
            except:
                return None
    else:
        return None


# read all text in the file, split string by space (' ')
# isolate all words as key, value pair, recode none alphabetic as None
# delete above declared items as remove_items

# increment value of the item as they appear on the whole set using reduceByKey function
word_count = input_file.flatMap(lambda line: line.split(' ')) \
                       .map(lambda word: [word_validate(word), 1]) \
                       .subtractByKey(remove_items) \
                       .reduceByKey(lambda a, b: a + b)

# merge all data into single rdd and write to text file
word_count.coalesce(1).saveAsTextFile(out_path)

# stop Spark
sc.stop()
