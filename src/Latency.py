#!/usr/bin/env python
__author__ = 'scott hendrickson'

from threading import RLock
import threading
import sys
import time
import datetime
try:
    import ujson as json
except:
    import json
from tweet_parser.tweet import Tweet

wrt_lock = RLock()

class Latency(threading.Thread):
    def __init__(self, _buffer, _feedname, _savepath, _rootLogger, _endTs, _spanTs, **kwargs):
        threading.Thread.__init__(self)
        with wrt_lock:
            self.logger = _rootLogger
            self.string_buffer = _buffer

    def run(self):
        with wrt_lock:
            self.logger.debug("started")
            for act in self.string_buffer.split("\n"):
                if act.strip() is None or act.strip() == '':
                    continue
                act_json = json.loads(act.strip())
                tweet = Tweet(act_json)
                now = datetime.datetime.utcnow()
                lat = now - tweet.created_at_datetime
                self.logger.debug("%s"%(lat))
                latSec = (lat.microseconds + (lat.seconds + lat.days * 86400.) * 10.**6) / 10.**6
                sys.stdout.write("%s, %f\n"%(now,latSec))
