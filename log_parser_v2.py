#! /usr/bin/env python3

import os
import re
import requests
from user_agents import parse
from collections import Counter

# comment to do not send
host = "http://ec2-18-191-90-115.us-east-2.compute.amazonaws.com:8428"

# comment to do not write csv file
outcsv = open("out.csv","w")
outcsv.write('time,location,provider,model,duration,cnt,session\n')

# delete or not input files
deleteafterread = False

csv_format = "1:time:unix_s,2:label:location,3:label:provider," + \
             "4:label:model," + \
             "5:metric:eduration,6:metric:ecnt,7:metric:esessions"


locations = {
    'MAG': 'Moscow Mag',
    'BUS': 'Moscow Bus',
    'EKT': 'Ekaterinburg',
    'NSK': 'Novosibirsk',
    'RND': 'Rostov-na-Donu',
}


# Input data example:
# StartDate','StartTime','EndDate','EndTime','IP','MediaID','SubscriberLogin','device','uTimeStart','uTimeEnd','Organization
# 2020/03/23','13:30:14','2020/03/23','23:45:46','85.140.18.64','3221229253','prod.451a8dbe483e4bb49cad58e2008d73aa','DmpPlayer/20.0.25.13_(Android_6.0.1;_98953f4d-8d86-4c84-969c-ba55f9dd64a2;_P024)','1584959414','1584996346','"Megafon"


def normalize_model(ua_string):
    if re.search(r'Apple ?TV', ua_string, flags=re.IGNORECASE):
        return 'AppleTV'
    elif re.search(r'[^a-zA-Z]TV[^a-zA-Z]', ua_string):
        return 'SmartTV'
    elif re.search(r'Android', ua_string):
        return 'Android'
    elif re.search(r'iPad', ua_string):
        return 'iPad'
    elif re.search(r'iPhone', ua_string):
        return 'iPhone'
    else:
        ua = parse(ua_string)
        if ua.is_pc:
            return 'PC'
        elif ua.is_tablet:
            return 'Tablet'
#        elif ua.is_mobile:
#            return 'Mobile'
        else:
            return 'Unknown'

def normalize_provider(p):
    if type(p) != str:
        return ""

    p = p.lower()
    if re.search(r'vimpel.?com', p):
        return "beeline"
    elif re.search(r'megafon', p):
        return "megafon"
    elif re.search(r'transtelecom', p):
        return "transtelecom"

    x3= ["[poc]?jsc", "llc", "ltd", "([a-z]+ )?joint stock company", "o[oa]o", "society with limited liability"]

    for r in x3:
        p = re.sub('^' + r+ ' ', '', p)
        p = re.sub(' ' + r+ '$', '', p)

    return p


def merge_by_time(d):
    d1 = {}
    for key, v in sorted(d.items(), key=lambda item: item[1]["time_end"]):
        time_end = v["time_end"]
        time_end = time_end-time_end % 60
        key1 = str(time_end) + ':' + v["model"] + ':' + v["provider"]
        if key1 in d1.keys():
            d1[key1]["cnt"] = d1[key1]["cnt"]+1
            d1[key1]["duration"] = d1[key1]["duration"]+v["duration"]
            d1[key1]["sessions"] = d1[key1]["sessions"]+v["sessions"]
        else:
            v["cnt"] = 1
            v["time_end"] = time_end
            d1[key1] = v

    return d1


def do_request(uri, post=None):
    if 'host' in vars() or 'host' in globals():
        if post is None:
            requests.get(host + uri)
        else:
            requests.post(host + uri, post)
    if 'outcsv' in vars() or 'outcsv' in globals() :
        if post is not None:
            outcsv.write(post)


def do_send(d, location):
    s = ""
    for key, v in d.items():
        s = s + "{},{},{},{},{},{},{}".format(v["time_end"], location, 
                                          v["provider"], v["model"],
                                          v["duration"], v["cnt"],
                                          v["sessions"]) + '\n'

        if len(s) > 100000:
            print(location + ': +' + str(len(s)) + ' bytes')
            do_request("/api/v1/import/csv?format=" + csv_format, s, )
            s = ""

    print(location + ': +' + str(len(s)) + ' bytes, done')
    do_request("/api/v1/import/csv?format=" + csv_format, s)

def readfile(filename):
    big = re.search(r'[A-Z]+', filename)
    if big is None:
        return

    host = big.group(0)
    if host in locations.keys():
        location = locations[host]
    else:
        location = host

    d = {}
    providers = {}

    i = 0
    with open(filename, 'r') as f:
        for line in f:
            result = re.split(r"','", line.strip())
            if len(result) == 11 and re.match(r'20\d\d/\d\d/\d\d', result[0]):
                provider = normalize_provider(re.search(r'"?(.*[^"])?"?', result[10]).group(1))
                if provider in providers.keys():
                    providers[provider] = providers[provider]+1
                else:
                    providers[provider] = 1
                i=i+1

#        print([(provider, cnt*100.0/i) for provider, cnt in  Counter(providers).most_common(20)])

        providers = [provider for provider, cnt in Counter(providers).most_common(20)]

        f.seek(0)
        for line in f:
            result = re.split(r"','", line.strip())
            if len(result) == 11 and re.match(r'20\d\d/\d\d/\d\d', result[0]):
                time_start = int(result[8])
                time_end = int(result[9])
                duration = time_end - time_start
                provider = normalize_provider(re.search(r'"?(.*[^"])?"?', result[10]).group(1))
                if provider not in providers:
                    provider = "other"

                model = normalize_model(re.sub(r'_', ' ', result[7]))
                key = result[6]+':'+result[5]
                if key in d.keys():
                    d[key]["duration"] = d[key]["duration"] + duration
                    d[key]["sessions"] = d[key]["sessions"] + 1
                    if d[key]["time_end"] < time_end:
                        d[key]["time_end"] = time_end
                else:
                    d[key] = {"duration": duration, "model": model,
                              "time_end": time_end, "sessions": 1,
                              "provider": provider}

    do_send(merge_by_time(d), location)


directory = os.fsencode(".")
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith(".log"):
        readfile(filename)
    if deleteafterread:
        os.remove(filename)

do_request("/internal/resetRollupResultCache")
