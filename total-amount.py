from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("TotalAmount")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(',')
    acctID = int(fields[0])
    amount = float(fields[2])
    return (acctID, amount)
    
    
lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
acctAmt  =  lines.map(parseLine)
totAmt = acctAmt.reduceByKey(lambda x,y: x+y)
totAmtSorted = totAmt.map(lambda x: (x[1], x[0])).sortByKey(1).map(lambda x: (x[1], x[0]))
results = totAmtSorted.collect()

#print("total records:  " + noofrows)
for result in results:
    print(result)
