from pandas import DataFrame
from pytrends.request import TrendReq
from time import sleep
from datetime import datetime
import requests



pytrends = TrendReq(hl='en-US')

all_keywords = '*' #In trends search '*' is considered as escape character which displys all keywords in a category

timeframes = ['today 5-y', 'today 12-m',
              'today 3-m', '2021-02-27 2021-03-27', '2021-03-27 2021-04-27']
#Below is the time frames in month wise for the past 10 years
timeintrevals = ["2011-01-01 2011-01-31", "2011-02-01 2011-02-28",
                 "2011-03-01 2011-03-31",
                 "2011-04-01 2011-04-30",
                 "2011-05-01 2011-05-31",
                 "2011-06-01 2011-06-30",
                 "2011-07-01 2011-07-31",
                 "2011-08-01 2011-08-31",
                 "2011-09-01 2011-09-30",
                 "2011-10-01 2011-10-31",
                 "2011-11-01 2011-11-30",
                 "2011-12-01 2011-12-31",
                 "2012-01-01 2012-01-31",
                 "2012-02-01 2012-02-29",
                 "2012-03-01 2012-03-31",
                 "2012-04-01 2012-04-30",
                 "2012-05-01 2012-05-31",
                 "2012-06-01 2012-06-30",
                 "2012-07-01 2012-07-31",
                 "2012-08-01 2012-08-31",
                 "2012-09-01 2012-09-30",
                 "2012-10-01 2012-10-31",
                 "2012-11-01 2012-11-30",
                 "2012-12-01 2012-12-31",
                 "2013-01-01 2013-01-31",
                 "2013-02-01 2013-02-28",
                 "2013-03-01 2013-03-31",
                 "2013-04-01 2013-04-30",
                 "2013-05-01 2013-05-31",
                 "2013-06-01 2013-06-30",
                 "2013-07-01 2013-07-31",
                 "2013-08-01 2013-08-31",
                 "2013-09-01 2013-09-30",
                 "2013-10-01 2013-10-31",
                 "2013-11-01 2013-11-30",
                 "2013-12-01 2013-12-31",
                 "2014-01-01 2014-01-31",
                 "2014-02-01 2014-02-28",
                 "2014-03-01 2014-03-31",
                 "2014-04-01 2014-04-30",
                 "2014-05-01 2014-05-31",
                 "2014-06-01 2014-06-30",
                 "2014-07-01 2014-07-31",
                 "2014-08-01 2014-08-31",
                 "2014-09-01 2014-09-30",
                 "2014-10-01 2014-10-31",
                 "2014-11-01 2014-11-30",
                 "2014-12-01 2014-12-31",
                 "2015-01-01 2015-01-31",
                 "2015-02-01 2015-02-28",
                 "2015-03-01 2015-03-31",
                 "2015-04-01 2015-04-30",
                 "2015-05-01 2015-05-31",
                 "2015-06-01 2015-06-30",
                 "2015-07-01 2015-07-31",
                 "2015-08-01 2015-08-31",
                 "2015-09-01 2015-09-30",
                 "2015-10-01 2015-10-31",
                 "2015-11-01 2015-11-30",
                 "2015-12-01 2015-12-31",
                 "2016-01-01 2016-01-31",
                 "2016-02-01 2016-02-29",
                 "2016-03-01 2016-03-31",
                 "2016-04-01 2016-04-30",
                 "2016-05-01 2016-05-31",
                 "2016-06-01 2016-06-30",
                 "2016-07-01 2016-07-31",
                 "2016-08-01 2016-08-31",
                 "2016-09-01 2016-09-30",
                 "2016-10-01 2016-10-31",
                 "2016-11-01 2016-11-30",
                 "2016-12-01 2016-12-31",
                 "2017-01-01 2017-01-31",
                 "2017-02-01 2017-02-28",
                 "2017-03-01 2017-03-31",
                 "2017-04-01 2017-04-30",
                 "2017-05-01 2017-05-31",
                 "2017-06-01 2017-06-30",
                 "2017-07-01 2017-07-31",
                 "2017-08-01 2017-08-31",
                 "2017-09-01 2017-09-30",
                 "2017-10-01 2017-10-31",
                 "2017-11-01 2017-11-30",
                 "2017-12-01 2017-12-31",
                 "2018-01-01 2018-01-31",
                 "2018-02-01 2018-02-28",
                 "2018-03-01 2018-03-31",
                 "2018-04-01 2018-04-30",
                 "2018-05-01 2018-05-31",
                 "2018-06-01 2018-06-30",
                 "2018-07-01 2018-07-31",
                 "2018-08-01 2018-08-31",
                 "2018-09-01 2018-09-30",
                 "2018-10-01 2018-10-31",
                 "2018-11-01 2018-11-30",
                 "2018-12-01 2018-12-31",
                 "2019-01-01 2019-01-31",
                 "2019-02-01 2019-02-28",
                 "2019-03-01 2019-03-31",
                 "2019-04-01 2019-04-30",
                 "2019-05-01 2019-05-31",
                 "2019-06-01 2019-06-30",
                 "2019-07-01 2019-07-31",
                 "2019-08-01 2019-08-31",
                 "2019-09-01 2019-09-30",
                 "2019-10-01 2019-10-31",
                 "2019-11-01 2019-11-30",
                 "2019-12-01 2019-12-31",
                 "2020-01-01 2020-01-31",
                 "2020-02-01 2020-02-29",
                 "2020-03-01 2020-03-31",
                 "2020-04-01 2020-04-30",
                 "2020-05-01 2020-05-31",
                 "2020-06-01 2020-06-30",
                 "2020-07-01 2020-07-31",
                 "2020-08-01 2020-08-31",
                 "2020-09-01 2020-09-30",
                 "2020-10-01 2020-10-31",
                 "2020-11-01 2020-11-30",
                 "2020-12-01 2020-12-31"]

cat = '45' #health category give inputas per your requirement
geo = ["IN-AP", "IN-AR", "IN-AS", "IN-BR", "IN-CT", "IN-GA", "IN-GJ", "IN-HR", "IN-HP", "IN-JH", "IN-KA", "IN-KL",
       "IN-MP", "IN-MH", "IN-MN", "IN-ML", "IN-MZ", "IN-NL", "IN-OR", "IN-PB", "IN-RJ",
       "IN-SK", "IN-TN", "IN-TG", "IN-TR", "IN-UP", "IN-UT", "IN-WB", "IN-AN", "IN-CH", "IN-DL", "IN-JK", "IN-PY"]
gprop = ''


def rel_queries(timeframe, g):
    i = 1
    while True:
        try:
            pytrends.build_payload(all_keywords,
                                   cat,
                                   timeframe,
                                   g,
                                   gprop)
            break
        except Exception as e:
            print(e, "Response error occured sleep for {} seconds".format(i * 20))
            sleep(i * 20)
            i += 1
            continue
    i = 1
    while True:
        try:
            data = pytrends.related_queries()
            break
        except Exception as e:
            print(e, "Response error occured in 2nd. sleep for {} seconds".format(i * 20))
            sleep(i * 20)
            i += 1
            continue
    return data


def search_topic(timeframe, g):
    i = 1
    while True:
        try:
            pytrends.build_payload(all_keywords,
                                   cat,
                                   timeframe,
                                   g,
                                   gprop)
            break
        except Exception as e:
            print(e, "Response error occured sleep for {} seconds".format(i * 20))
            sleep(i * 20)
            i += 1
            continue
    i = 1
    while True:
        try:
            data = pytrends.related_topics()
            break
        except Exception as e:
            print(e, "Response error occured sleep for {} seconds".format(i * 20))
            sleep(i * 20)
            i += 1
            continue

    return data

if __name__ =='__main__'
#exporting the datas to CSV file for future Use
#created loop of 120 months and all states in india

    print('Start Time: ', datetime.now().time())
    for state in geo: # Modify here depends on the region required
        for timeintreval in timeintrevals:  # Modify here depends on the Time required
            queries = rel_queries(timeintreval, state)
            #print(out1)
            topics = search_topic(timeintreval, state)
            for j in ['top', 'rising']:
                try:
                    out1['*'][j]['type'] = j
                    out1['*'][j]['time_interval'] = timeintreval
                    out1['*'][j]['region'] = state
                    print(out1)
                except Exception as e:
                    print(e,'None exception found in queries')
                    print(out1)
                try:
                    out1['*'][j].to_csv('search_queries.csv', mode='a', header=False)
                except Exception as e:
                    print(e, 'error occured')
                    print('state and time is: ', state, time_interval)
                topics=['*'][j]['type'] = j
                topics['*'][j]['time_interval'] = timeintreval
                topics['*'][j]['region'] = state
                print(topics)
                try:
                    topics['*'][j][['value','formattedValue','link','topic_mid','topic_title','topic_type','type','time_interval','region']].to_csv('Search_Topics.csv', mode='a', header=False)
                except Exception as e:
                    print(f' Expetion during topic export for state: {state} time: {timeintreval}')
                    topics['*'][j].to_csv("except_Topic.csv", mode='a', header=False)  #appending in a file for future use


    print('Finish Time', datetime.now().time())
