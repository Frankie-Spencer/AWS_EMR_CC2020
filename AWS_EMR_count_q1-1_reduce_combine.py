import sys
from pyspark import SparkContext, HiveContext

sc = SparkContext()
sqlContext = HiveContext(sc)

# system input arguments needed
# e. g /dir/text.txt /dir/out 100 'reduce'
args = sys.argv
source_path, out_path, amount, process_type = args[1], args[2], args[3], args[4]

# declare removable items
# -empty strings and which could be created when splitting
# -None values as created if the word doesn't include alphabet
remove_items = sc.parallelize([(None, None)])

# import input file, and set use_unicode to True to recognise Finnish letters
input_file = sc.textFile(source_path, use_unicode=True)

# read all text in the file, split string by space (' ')
# isolate all words as key, value pair, recode none alphabetic as None
# delete above declared items as remove_items

# increment value of the item as they appear on the whole set using reduceByKey function
if process_type == 'reduce':
    word_count = input_file.flatMap(lambda line: line.split(' '))\
                           .map(lambda word: [word if any(s.isalpha() for s in word) else None, 1])\
                           .subtractByKey(remove_items)\
                           .reduceByKey(lambda a, b: a + b)

# increment value of the item as they appear on the whole set using combineByKey function
elif process_type == 'combine':
    word_count = input_file.flatMap(lambda line: line.split(' ')) \
                           .map(lambda word: [word if any(s.isalpha() for s in word) else None, 1]) \
                           .subtractByKey(remove_items) \
                           .combineByKey(lambda v: v,
                                         lambda c, v: c + v,
                                         lambda c1, c2: c1 + c2)


# make a function to correct invalid inputs to be equal to total entries
# and users can enter 0 if they want full list, as its usually the standard
def output_amount(n):
    try:
        num = int(n)
        if num <= 0:
            return word_count.count()
        else:
            return num
    except:
        return word_count.count()


# order by value descending and get only given value by argument
word_count_limit_sorted = word_count.takeOrdered(output_amount(amount), key=lambda x: -x[1])

# merge all data into single rdd
final_output = sc.parallelize(word_count_limit_sorted)\
                 .coalesce(1)

# write to text file
final_output.saveAsTextFile(out_path)

# stop Spark
sc.stop()
