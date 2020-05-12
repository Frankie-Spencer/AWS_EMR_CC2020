import sys
from math import floor, pow, log
from pyspark import SparkContext, HiveContext

sc = SparkContext()
sqlContext = HiveContext(sc)

# system input argument needed
# e. g /dir/text.txt /dir/out
args = sys.argv
source_path, out_path = args[1], args[2]

# declare removable items
# -empty strings and which could be created when splitting
# -None values as created if the word doesn't include alphabet
remove_items = sc.parallelize([(None, None)])

# import input file, and set use_unicode to True to recognise Finnish letters
input_file = sc.textFile(source_path, use_unicode=True)

# declare given prices
request_price = 0.001
data_price = 0.08


# this function will calculate cost at the same time and store values
# denote user ip as key and values as [requests, bytes, cost]
# set 1 as session amount for this request, store amount of bytes as they are
# calculate cost by using ((bytes / bytes per gb) * price per gb) + request price
# save cost in 3rd column
def get_key_values(d):
    data_k_v = d.split(' ')
    try:
        int(data_k_v[9])
        data_cost = ((int(data_k_v[9]) / 1073741824) * data_price) + request_price
        return [data_k_v[0], [1, int(data_k_v[9]), data_cost]]
    except:
        return [data_k_v[0], [1, 0, request_price]]


# read all text in the file, split string by linebreak ('\b')
# get key and value from above function
# delete above declared items as remove_items (None values)
# increment value of the item as they appear on the whole set using reduceByKey function
web_server_data = input_file.flatMap(lambda line: line.split('\b'))\
                            .map(lambda data: get_key_values(data)) \
                            .reduceByKey(lambda a, b: [a[0] + b[0],
                                                       a[1] + b[1],
                                                       a[2] + b[2]])


# this function will transform bytes to KB, MB etc
def convert_size(size_bytes):
    if size_bytes == 0:
        return '0B'
    size_ab = ['b', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    i = int(floor(log(size_bytes, 1024)))
    p = pow(1024, i)
    s = round(size_bytes / p, 2)

    return '%s%s' % (s, size_ab[i])


# map data once more to rewrite bytes on proper format using above function
# round op the final cost
# merge all data into single rdd
web_server_data = web_server_data.map(lambda data: (data[0], data[1][0],
                                                    convert_size(data[1][1]),
                                                    round(data[1][2], 4))).coalesce(1)

# write to text file
web_server_data.saveAsTextFile(out_path)

# stop Spark
sc.stop()
