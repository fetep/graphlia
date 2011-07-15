#!/usr/bin/env python2.6

from amqplib import client_0_8 as amqp
from xdrlib import Unpacker
import ConfigParser
import SocketServer
import socket
import sys
import threading
import time

# All these types expect a metric_id struct & fmt string first
GANGLIA_DECODE = {
#    129: lambda(unpacker): unpacker.unpack_ushort(),
#    130: lambda(unpacker): unpacker.unpack_short(),
    131: lambda(unpacker): unpacker.unpack_int(),
    132: lambda(unpacker): unpacker.unpack_uint(),
    134: lambda(unpacker): unpacker.unpack_float(),
    135: lambda(unpacker): unpacker.unpack_double(),
}

GANGLIA_LISTEN_PORT = 8651

class GangliaCollector(SocketServer.BaseRequestHandler):
    def handle(self):
        data = self.request[0]

        unpacker = Unpacker(data)
        type = unpacker.unpack_int()
        if type not in GANGLIA_DECODE: return

        host = unpacker.unpack_string()
        name = unpacker.unpack_string()
        unpacker.unpack_int() # spoof boolean
        unpacker.unpack_string() # format string
        value = GANGLIA_DECODE[type](unpacker)
        unpacker.done()

        graphite.record_stat(name, value)

class GraphiteAggregator(object):
    def __init__(self, host, cluster, mapping, rates, amqp_conn, amqp_exchange):
        self.host = host
        self.cluster = cluster
        self.mapping = mapping
        self.use_rates = rates
        self.amqp_conn = amqp_conn
        self.amqp_exchange = amqp_exchange
        self.amqp_chan = self.amqp_conn.channel()

        self.last_value = {}
        self.last_time = {}
        self.stats_lock = threading.Lock()
        self.stats = []

        update_thread = threading.Thread(target=self.send_updates_thread)
        update_thread.setDaemon(True)
        update_thread.start()

    def record_stat(self, orig_name, value):
        if orig_name not in self.mapping: return
        name = self.mapping[orig_name]
        now = time.time()
        if name in self.use_rates:
            if name not in self.last_value:
                self.last_value[name] = value
                self.last_time[name] = now
                return
            if value < self.last_value[name]:
                # counter rollover?
                self.last_value[name] = value
                self.last_time[name] = now
                return
            rate = (value - self.last_value[name]) / \
                   (now - self.last_time[name])
            self.last_value[name] = value
            self.last_time[name] = now
            value = rate

        # lock, append to updatequeue name/value, unlock
        self.stats_lock.acquire()
        self.stats.append((name, value, now))
        self.stats_lock.release()

    def send_updates_thread(self):
        while True:
            time.sleep(10)
            try:
                self.send_updates()
            except Exception as e:
                print >>sys.stderr, "Error sending updates: %s" % (e,)

    def send_updates(self):
        if len(self.stats) == 0: return

        self.stats_lock.acquire()
        stats = self.stats[:]
        self.stats = []
        self.stats_lock.release()

        output = []
        for name, value, now in stats:
            output.append(self._stat(name, value, now))

        print "\n".join(output)
        msg = amqp.Message("\n".join(output))
        try:
            self.amqp_chan.basic_publish(msg, exchange=self.amqp_exchange)
        except socket.error as (errno, errstr):
            print >> sys.stderr, "amqp publish problem: %s" % (errstr,)
            sys.exit(2)

    def _stat(self, name, value, now):
        return "%s.%s.%s %f %d" % \
               (name, self.cluster, self.host, value, int(now))

if __name__ == "__main__":
    config = ConfigParser.RawConfigParser()
    if len(sys.argv) > 1:
        config.read(sys.argv[1])
    else:
        config.read("/etc/graphlia.ini")

    mapping = {}
    use_rates = []
    for line in config.get("gmond", "mapping").split("\n"):
        if line == "": continue
        parts = line.split(":")
        if len(parts) != 2 and len(parts) != 3:
            raise ValueError("mapping #{line} must be of form gmond:ganglia[:rate]")
        mapping[parts[0]] = parts[1]
        if len(parts) == 3 and parts[2] == "rate":
            use_rates.append(parts[1])

    if len(mapping) == 0:
        raise ValueError("gmond.mapping is missing in the config file")

    host = config.get("collector", "host")
    colo = config.get("collector", "colo")

    amqp_conn = amqp.Connection(host=config.get("amqp", "host"),
                                userid=config.get("amqp", "user"),
                                password=config.get("amqp", "pass"),
                                virtual_host=config.get("amqp", "vhost"),
                                insist=False)
    amqp_exchange = config.get("amqp", "exchange")

    graphite = GraphiteAggregator(host, colo, mapping, use_rates,
                                  amqp_conn, amqp_exchange)

    server = SocketServer.UDPServer(("127.0.0.1", GANGLIA_LISTEN_PORT),
                                    GangliaCollector)
    server.serve_forever()
