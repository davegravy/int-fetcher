
import random
import uuid
import argparse
import sys
import os
import pyquist as pq
import json
from datetime import datetime, timedelta
from time import sleep

from twisted.internet import reactor, threads
#from twisted.internet import task
from twisted.web import server, resource
# from twisted.internet.defer import Deferred
#from twisted.web.client import Agent, readBody

monitor_data = {}
requests = {}

class Root(resource.Resource):
    isLeaf = False

    def render_GET(self, request):

        request.setHeader('Access-Control-Allow-Origin', '*')
        return "arkanoid?".encode()


class IntervalFetcher(resource.Resource):
    isLeaf = False

    def render_GET(self, request):

        request.setHeader('Access-Control-Allow-Origin', '*')
        return "arkanoid??".encode()

class GetIntervals(resource.Resource):
    isLeaf = True

    def render_POST(self, request):

        #todo check for ?and filter? asterisks in args
        if b'site_key' in request.args:

            site = request.args[b'site_key'][0].decode()

            if len(site) < 1: site = "*"
        else: site= "*"

        if b'device_id' in request.args:
            device = request.args[b'device_id'][0].decode()
            if len(device) < 1: device = "*"
        else: device= "*"

        if b'start_time' in request.args:
            start_time = request.args[b'start_time'][0].decode()
            if len(start_time) < 1:
                print("no start time supplied")
                start_time="0"

            start_time= int(start_time)

        else:
            #todo determine action
            start_time=0
            print("no start time supplied")




        print ("Device: {device} Site: {site} Start Time: {start_time}".format(device=device, site=site, start_time=start_time))


        if not device in monitor_data:
            monitor_data[device] = {}

        new_data = []



        start_datetime = datetime.fromtimestamp(start_time)
        iterated_datetime = start_datetime

        import time
        t0 = time.time()

        while True:
            iterated_hour = iterated_datetime.replace(minute=0, second=0)
            if iterated_hour >= datetime.now():
                print("end of loop")
                break

            # print(iterated_datetime.strftime("%Y"))
            # print(iterated_datetime.strftime("%m") + "-" + start_datetime.strftime("%B"))
            # print(iterated_datetime.strftime("%d"))
            # print(iterated_datetime.strftime("%H"))
            # print(iterated_datetime.strftime("%M"))
            # print(iterated_datetime.strftime("%S"))
            # print(iterated_datetime.strftime("%f"))
            # print("")
            # print("")
            # print("iterated hour: ")
            # print(iterated_hour.strftime("%Y"))
            # print(iterated_hour.strftime("%m") + "-" + start_datetime.strftime("%B"))
            # print(iterated_hour.strftime("%d"))
            # print(iterated_hour.strftime("%H"))
            # print(iterated_hour.strftime("%M"))
            # print(iterated_hour.strftime("%S"))
            # print(iterated_hour.strftime("%L"))
            # print("")
            # print("")

            pattern = ('s3://switchboard-dev.aercoustics.com/{site}/{device}/{year}/{month}/{day}/{hour}/'
                       '*T*/*T*_sINT.JSON')\
                .format(site=site, device=device, year=iterated_datetime.strftime("%Y"),
                        month=iterated_datetime.strftime("%m") + "-" + start_datetime.strftime("%B"),
                        day=iterated_datetime.strftime("%d"), hour=iterated_datetime.strftime("%H"))

            print(pattern)

            keys = pq.io.match(pattern)

            print ('got keys: ' + str(time.time() - t0))


            for key in keys:
                # print(key)
                if not key in monitor_data[device]:
                    # print ("new key: " + key)
                    data = pq.io.read_json(key)
                    # new_data.append(data)
                    monitor_data[device][key] = data
                    # else:
                    #     print ("cached key")
            print('got data: ' + str(time.time() - t0))



            iterated_datetime = iterated_datetime + timedelta(hours=1)
            #stuff


        # do the thing!
        result = json.dumps(monitor_data[device])
        #todo add pagination
        return result.encode()

class GetIntervals2(resource.Resource):
    isLeaf = True

    device = None
    site = None
    start_time = None
    request_id = None

    def get_args(self, request):

        #todo check for ?and filter? asterisks in args
        if b'site_key' in request.args:

            self.site = request.args[b'site_key'][0].decode()

            if len(self.site) < 1: self.site = "*"
        else: self.site= "*"

        if b'device_id' in request.args:
            self.device = request.args[b'device_id'][0].decode()
            if len(self.device) < 1: self.device = "*"
        else: self.device= "*"

        if b'start_time' in request.args:
            self.start_time = request.args[b'start_time'][0].decode()
            if len(self.start_time) < 1:
                print("no start time supplied")
                self.start_time="0"

            self.start_time= int(self.start_time)

        else:
            #todo determine action
            self.start_time=0
            print("no start time supplied")

        if (b'request_id' not in request.args) or (request.args[b'request_id'][0].decode() not in requests):
            self.request_id = uuid.uuid4().hex
            requests[self.request_id] = {'completed': False}  # the data may or may not all be cached at this point

        else:
            self.request_id = request.args[b'request_id'][0].decode()
            print('resuming request ID: ' + str(self.request_id))

        print("Device: {device} Site: {site} Start Time: {start_time} Request ID: {request_id}"
              .format(device=self.device, site=self.site, start_time=self.start_time, request_id=self.request_id))

    def get_keys(self):
        if 'keys' not in requests[self.request_id]:
            keys = []

            start_datetime = datetime.fromtimestamp(self.start_time)
            iterated_datetime = start_datetime

            while True:
                iterated_hour = iterated_datetime.replace(minute=0, second=0)
                if iterated_hour >= datetime.now():
                    print("end of loop")
                    break

                pattern = ('s3://switchboard-dev.aercoustics.com/{site}/{device}/{year}/{month}/{day}/{hour}/'
                           '*T*/*T*_sINT.JSON') \
                    .format(site=self.site, device=self.device, year=iterated_datetime.strftime("%Y"),
                            month=iterated_datetime.strftime("%m") + "-" + start_datetime.strftime("%B"),
                            day=iterated_datetime.strftime("%d"), hour=iterated_datetime.strftime("%H"))

                print(pattern)

                keys.extend(pq.io.match(pattern))

                print(len(keys))

                iterated_datetime = iterated_datetime + timedelta(hours=1)

            requests[self.request_id]['keys'] = keys

    def launch_cache_thread(self):

        print("about to start cacheing")
        # reactor.callWhenRunning(cacheRequestedData, keys, device)

        if not self.device in monitor_data:  # initialize the cache if needed
            monitor_data[self.device] = {}

        if 'worker' not in requests[self.request_id] and requests[self.request_id]['completed'] == False:
            print("creating new cache worker")
            requests[self.request_id]['worker'] = threads.deferToThread(cache_requested_data, self.request_id,
                                                                        self.device)
            requests[self.request_id]['worker'].addCallback(cache_request_callback, self.request_id)
            requests[self.request_id]['worker'].addErrback(cache_request_errback, self.request_id)
        else:
            print("worker is already registered to cache this request or cacheing already complete")
            # cacheRequestedData2(loop, keys, device)

    def get_cached_data(self):

        result = {'keys': {}}

        complete_flag = True

        for key in requests[self.request_id]['keys']:
            # print(key)
            if key in monitor_data[self.device]:
                result['keys'][key] = monitor_data[self.device][key]

            else:
                complete_flag = False

        result['request_id'] = self.request_id
        result['complete'] = complete_flag
        return result

    def render_POST(self, request):

        self.get_args(request) #parse supplied args from request

        self.get_keys() #determine the S3 keys that correspond to request

        self.launch_cache_thread()

        result = self.get_cached_data()

        rtrn = json.dumps(result).encode('utf8')
        print ('result size BYTES: ' + str(len(rtrn)))
        return rtrn







def cache_request_callback(result, request_id):

    print("in cacheRequestCallback")
    print ('{request_id} complete'.format(request_id=request_id))

def cache_request_errback(result, request_id):

    print("in cacheRequestErrback")
    print (result.getTraceback())
    print('{request_id} FAILURE'.format(request_id=request_id))
    requests[request_id]['worker'] = {} #unregister worker, it is dead.

def cache_requested_data(request_id, device):

    print('started cacheing for {device}'.format(device=device))

    if requests[request_id]['completed']:
        print ('request already cached')
        return

    for key in requests[request_id]['keys']:

        if not key in monitor_data[device]:
            # print ("new key: " + key)
            data = pq.io.read_json(key)
            # new_data.append(data)
            monitor_data[device][key] = data
            for interval in monitor_data[device][key]['ARMS-NVM data']['weather']['measurement data']: #todo remove
                monitor_data[device][key]['ARMS-NVM data']['weather']['measurement data'][interval]['average wind speed'] = str(random.uniform(0,10))

    print('finished cacheing')
    requests[request_id]['completed'] = True
    return

def parse_args():

    parser = argparse.ArgumentParser(description="caching interval fetcher server")
    parser.add_argument("-p", "--port", help="the port for fes-proxy server to listen on", default=8000, type=int)
    # parser.add_argument("-pi", "--pole-interval", help="how frequently (in seconds) to poll monitors "
    #                                                    "for values", default=5, type=int)
    parser.add_argument("-ct", "--cache-time", help="how long (in seconds) to cache monitor values "
                                                    "after the latest request", default=60, type=int)
    parser.add_argument("-v", "--verbose", help="verbose output", action="store_true")

    args = parser.parse_args()

    return args.port, args.cache_time

def main():

    if not os.path.dirname(sys.argv[0]) == '':
        os.chdir(os.path.dirname(sys.argv[0]))  # change working directory to script location
    # so that log files end up in the script path, not fs root.

    global cache_time, polling_interval, sites
    listen_port, cache_time = parse_args()

    print ("   Listen Port: " + str(listen_port))
    # print ("Polling Period: " + str(polling_interval))
    print ("    Cache Time: " + str(cache_time))

    root = Root()
    interval_fetcher = IntervalFetcher()
    root.putChild("".encode('utf-8'), Root())
    root.putChild("int-fetcher".encode('utf-8'), interval_fetcher)
    interval_fetcher.putChild("getIntervals".encode('utf-8'), GetIntervals())  # experimental
    interval_fetcher.putChild("getIntervals2".encode('utf-8'), GetIntervals2())  # experimental
    # proxy.putChild("verifyToken".encode('utf-8'), VerifyToken())
    # proxy.putChild("getMonitorValues".encode('utf-8'), GetMonitorValues())
    # proxy.putChild("setStartInstrument".encode('utf-8'), SetStartInstrument())
    # proxy.putChild("setStopInstrument".encode('utf-8'), SetStopInstrument())
    # proxy.putChild("setPowerOnInstrument".encode('utf-8'), SetPowerOnInstrument())
    # proxy.putChild("setPowerOffInstrument".encode('utf-8'), SetPowerOffInstrument())
    # #proxy.putChild("Login", Login())
    # proxy.putChild("getSites".encode('utf-8'), GetSites())
    site = server.Site(root)

    # from twisted.python.modules import getModule
    #from twisted.internet import reactor
    # from twisted.python.filepath import FilePath

    #  certData = getModule(__name__).filePath.sibling('server.pem').getContent()
    # cert_data = FilePath('/home/ubuntu/arms-fes-proxy/server.pem').getContent()
    # certificate = ssl.PrivateCertificate.loadPEM(cert_data)

    # reactor.listenSSL(listen_port, site, certificate.options())
    reactor.listenTCP(listen_port, site)

    print("reactor listening...")
    # list_call1 = task.LoopingCall(send_wscs)
    # list_call2 = task.LoopingCall(Monitor.flush_monitor_cache)
    # list_call3 = task.LoopingCall(update_sites)
    #
    # list_call1.start(polling_interval)
    # list_call2.start(polling_interval)
    # list_call3.start(900)

    #reactor.callLater(5, test_func)

    print("running reactor")
    reactor.run()

if __name__ == '__main__':
    main()