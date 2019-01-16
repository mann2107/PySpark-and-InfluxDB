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
    x = xval.split('|')[0]
    y = yval.split('|')[0]
    ytime = str(yval.split('|')[1])
    if str(x) == 'None':
        x = 0
    if str(y) == 'None':
        y = 0
    d = abs(float(y) - float(x))
    # ytime = getInfluxTime(ytime)
    return str(d) + '|' + str(ytime)


def create_json(value):
    pn, temp = value
    t = temp.split('|')[0]
    t = float(t)
    time = temp.split('|')[1]

    parent = {"measurement": "test_mean-temperature-counter",
              "tags": {
                  "host": "server01",
                  "region": "us-west",
                  "source": "BDS"
                  },
              "time": time
              }
    d = {}
    d['source'] = 'BDS'
    d['point_name'] = pn
    d['time'] = time
    if 1.8 <= t < 3.6:
        d['c1.8'] = 1
        d['c3.6'] = None
        d['c18'] = None
        d['c36'] = None
    elif 3.6 <= t < 18:
        d['c1.8'] = None
        d['c3.6'] = 1
        d['c18'] = None
        d['c36'] = None
    elif 18 <= t < 36:
        d['c1.8'] = None
        d['c3.6'] = None
        d['c18'] = 1
        d['c36'] = None
    elif 36 <= t:
        d['c1.8'] = None
        d['c3.6'] = None
        d['c18'] = None
        d['c36'] = 1
    else:
        d['c1.8'] = None
        d['c3.6'] = None
        d['c18'] = None
        d['c36'] = None

    parent['fields'] = d

    return parent


def get_influx_time(date):

    date_time_str = date
    date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%dT%H:%M:%SZ')

    timezone = pytz.timezone('Zulu')
    timezone_date_time_obj = timezone.localize(date_time_obj)
    x = int(mktime(timezone_date_time_obj.timetuple()))
    x = x * 1000000000
    return x

if __name__ == '__main__':
    client = InfluxDBClient(host='dvmaprapp02corp.eprod.com', port=8086)

    client.switch_database('PI')
    rs = client.query('SELECT mean("temp") FROM "autogen"."temperature" WHERE "point_name"=\'220PDI0902.PV\' GROUP BY point_name, site, equipment, time(1m) fill(linear) limit 500')
    data = sc.parallelize(rs)
    name_value = data.flatMap(lambda x: x).map(lambda x: ('220PDI0902', str(x['mean']) + '|' + x['time']))
    res = name_value.reduceByKey(diff).map(create_json).collect()
    pp = pprint.PrettyPrinter(indent=4)
    client.write_points(res)
    pp.pprint(res)
    res = name_value.collect()
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(res)





