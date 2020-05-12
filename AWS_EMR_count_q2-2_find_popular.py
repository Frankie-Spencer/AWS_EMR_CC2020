import sys
from pyspark import SparkContext, HiveContext

sc = SparkContext()
sqlContext = HiveContext(sc)

# system input arguments needed
# e. g /dir/text.txt /dir/out 5
args = sys.argv
source_path, out_path, amount = args[1], args[2], args[3]

# declare removable items
# -empty strings and which could be created when splitting
# -None values as created if the word doesn't include alphabet
remove_items = sc.parallelize([(None, None)])

# import input file, and set use_unicode to True to recognise Finnish letters
input_file = sc.textFile(source_path, use_unicode=True)


# this function will validate if the criteria met for mapping
# fist round of mapping with argument 'users' conditions: only ips with domain names
# return user-ip name as key and 1 as value
# one doesn't meet the parameter will be denoted key and value as None
# next round of mapping data expected meets the criteria because they are already validated
# return only domain as key and 1 as value
def get_key_values(d, q):

    if q == 'users':
        data_k_v = d.split(' ', 1)[0]
        if any(s.isalpha() for s in data_k_v):
            if any(s == '.' for s in data_k_v):
                return [data_k_v, 1]
            else:
                return [None, None]
        else:
            return [None, None]
    elif q == 'domains':
        domain = d.split('.', 1)[1]
        return [domain, 1]


# read all text in the file, split string by linebreak ('\b')
# get key and value from above function
# delete above declared items as remove_items (None values)
# increment value of the item as they appear on the whole set using reduceByKey function
web_server_data = input_file.flatMap(lambda line: line.split('\b'))\
                            .map(lambda data: get_key_values(data, 'users')) \
                            .subtractByKey(remove_items)\
                            .reduceByKey(lambda a, b: a + b)\
                            .map(lambda data: get_key_values(data[0], 'domains'))\
                            .reduceByKey(lambda a, b: a + b)


# make a function to correct invalid inputs to be equal to total entries
# and users can enter 0 if they want full list, as its usually the standard
def output_amount(n):
    try:
        num = int(n)
        if num <= 0:
            return web_server_data.count()
        else:
            return num
    except:
        return web_server_data.count()


# order by value descending and get only given value by argument
web_server_data = web_server_data.takeOrdered(output_amount(amount), key=lambda x: -x[1])

# merge all data into single rdd
final_output = sc.parallelize(web_server_data).coalesce(1)

# write to text file
final_output.saveAsTextFile(out_path)

# stop Spark
sc.stop()
