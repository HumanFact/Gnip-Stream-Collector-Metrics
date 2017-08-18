#!/usr/bin/env python
__author__ = 'scott hendrickson'

import sys
from SaveThread import SaveThread
from threading import RLock
try:
    import ujson as json
except ImportError:
    import json
import datetime
import gzip
from tweet_parser.tweet import Tweet

wrt_lock = RLock()

class CountTwitterRules(SaveThread):
    def write(self, file_name):
        self.logger.debug("CountTwitterRules is writing.")
        count_map = {}
        rule_tags = {}
        count = 0
        for act in self.string_buffer.split("\n"):
            #self.logger.debug(str(act))
            if act.strip() is None or act.strip() == '':
                continue
            count +=1
            act_json = json.loads(act.strip())
            tweet = Tweet(act_json)
            if tweet.gnip_matching_rules is not None:
                for mr in tweet.gnip_matching_rules:
                    rule = mr["id"]
                    rule_tags[mr["id"]] = mr["tag"]
                    #self.logger.debug(str(rule))
                    if rule in count_map:
                        count_map[rule] += 1
                    else:
                        count_map[rule] = 1
            else:
                self.logger.error("matching_rules missing")
        
        try:
            now = datetime.datetime.now()
            file_name = file_name.replace("gz","counts")
            fp = open(file_name, "a")
            sys.stdout.write("(%s) sample %d tweets (%d seconds)\n"%
                    (datetime.datetime.now(), count, self.timeSpan))
            first_col = max([len(str(x)) for x in count_map.keys()]) + 3
            count_mapKeys = sorted(count_map.keys(), key=count_map.__getitem__)
            count_mapKeys.reverse()
            for r in count_mapKeys:
                # for dump to file
                write_str = "{}, {}, {}, {}, {}\n".format(now,
                                                          r,
                                                          rule_tags[r],
                                                          count_map[r],
                                                          count,
                                                          self.timeSpan)
                fp.write(write_str)
                # for dump to stdout
                sys.stdout.write("%s: %s (%4d tweets matched)   %3.4f tweets/second\n"%
                    (r, '.'*(first_col-len(str(r))), count_map[r], float(count_map[r])/self.timeSpan)) 
            fp.close()
            self.logger.info("saved file %s"%file_name)
        except Exception as e:
            self.logger.error("write failed: %s"%e)
            raise e

