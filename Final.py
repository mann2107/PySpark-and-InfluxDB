import pprint
import os
import datetime
import pytz
from time import mktime
from influxdb import InfluxDBClient
from pyspark import SparkConf, SparkContext

# Configure the environment
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/home/ubuntu/spark-1.6.0-bin-hadoop2.6'

conf = SparkConf().setAppName('pubmed_open_access').setMaster('local[32]')
sc = SparkContext(conf=conf)


def diff(xval, yval):
    # xval is value from row 1
    # yval is value from row 2
    # Extract mean from row 1
    x = xval['mean']
    # Extract mean from row 2
    y = yval['mean']
    # Extract time from row 2 (from row 2 as this will be the latest time)
    ytime = yval['time']
    # Check x and y for None, if found replace with 0
    if str(x) == 'None':
        x = 0
    if str(y) == 'None':
        y = 0
    # Calculate absolute difference,
    d = abs(float(y) - float(x))
    # ytime = getInfluxTime(ytime)
    # Create empty dictionary to collect result and return
    res = {}
    # Push time into result
    res['time'] = ytime
    # Push absolute difference into result
    res['mean'] = d
    # Return the result
    return res


def create_json(value, point_name):
    # Extract mean from value
    temp = value['mean']
    # Check and replace None value with 0
    if str(temp) == 'None':
        temp = 0
    # Convert mean into float for comparison
    t = float(temp)
    # Extract time from value
    time = value['time']
    # Define Parent JSON, with measurement, tags and time (Fields to be pushed later)
    parent = {"measurement": "final-counter-group",
              "tags": {"source": "bds", "point_name": point_name},
              "time": time
              }
    # Make an empty dictionary for collecting fields
    d = {}
    #d['point_name'] = point_name
    # c1.8, c3.6, c18, c36 logic
    if 1.8 <= t < 3.6:
        d['c1.8'] = 1
    elif 3.6 <= t < 18:
        d['c3.6'] = 1
    elif 18 <= t < 36:
        d['c18'] = 1
    elif 36 <= t:
        d['c36'] = 1
    else:
        d['c1.8'] = 'No Change'
        d['c3.6'] = 'No Change'
        d['c18'] = 'No Change'
        d['c36'] = 'No Change'

    # Push fields into the parent JSON
    parent['fields'] = d
    # Return the results
    return parent


if __name__ == '__main__':
    # Create client for InfluxDB
    client = InfluxDBClient(host='dvmaprapp02corp.eprod.com', port=8086)
    # Use DB PI
    client.switch_database('PI')
    # Define the query
    a = 'SELECT mean("temp") FROM "temperature" WHERE ("equipment" = \'E-0005\') AND time >= 1532461536849ms and time <= 1532483815125ms GROUP BY time(1m), "point_name" fill(linear)'
    # Run query and collect result in a variable rs
    rs = client.query(a)
    # Define the style for pretty printer for fancy printing of results
    pp = pprint.PrettyPrinter(indent=4)
    # Print the result
    pp.pprint(rs)
    # Collect all keys from the data returned from InfluxDB
    keys = rs.keys()

    # Create an empty list to collect final result to be inserted back into InfluxDB
    dp = []
    # Loop over all the keys
    for i in keys:
        # Unpack the tuple (measurement, value), keys are returned from DB in this format
        m, v = i
        # Extract point_name from value
        p = v['point_name']
        # Retrieve all points(list of time and mean temperature ) belonging to current point_name
        d = rs.get_points(tags={'point_name': p})
        # Print the point_name
        pp.pprint('Writing points for ' + str(p))
        # Print all the points
        pp.pprint(d)
        # Create spark rdd from points
        data = sc.parallelize(d)
        # Apply reduce to get commutative absolute difference
        name_value = data.reduce(diff)
        # Print the result of reduce
        pp.pprint('Reduced as ' + str(name_value))
        # Make JSON with result, ready to insert into DB, as per DB's specification
        res = create_json(name_value, p)
        # Append to list to be inserted as a batch later
        dp.append(res)

    # Print the batch list of JSONs
    pp.pprint(dp)
    # Write data into DB
    client.write_points(dp)






