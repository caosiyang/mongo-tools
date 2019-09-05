#! /usr/bin/env python
# -*- coding: UTF-8 -*-

""" Check status of members in replica-set/sharded-cluster.
"""

import sys
import argparse
import pymongo


g_node = '127.0.0.1:27017'
g_host = '127.0.0.1'
g_port = 27017
g_username = ''
g_password = ''


def parse_hostportstr(hostportstr):
    """ hostportstr LIKE 'host:port'
    """
    s = hostportstr.strip()
    if s.find(':') > 0:
        vals = s.split(':')
        host = vals[0]
        port = int(vals[1])
        return (host, port)
    else:
        return (s, None)


def parse_arguments():
    global g_node, g_host, g_port, g_username, g_password

    parser = argparse.ArgumentParser(description="check status of members in replica-set/sharded-cluster")
    parser.add_argument('node', nargs='?', help='a node that could be mongos/mongod/member of replica-set, host:port')
    parser.add_argument('-u', nargs='?', required=False, help='username')
    parser.add_argument('-p', nargs='?', required=False, help='password')

    args = vars(parser.parse_args())
    if args['node'] is not None:
        g_node = args['node']
        if g_node.find(':') > 0:
            g_host, g_port = parse_hostportstr(g_node)
        else:
            g_host = g_node
    if args['u'] is not None:
        g_username = args['u']
    if args['p'] is not None:
        g_password = args['p']


def connect(host, port, **kwargs):
    """ Connect and return a available handler.
    Default authentication database is 'admin'.
    """
    username = kwargs.get('username', '')
    password = kwargs.get('password', '')
    mc = pymongo.MongoClient(host, port, serverSelectionTimeoutMS=1000)
    if username and password:
        mc.admin.authenticate(username, password)
    return mc


def get_mongo_type(host, port, **kwargs):
    """ Get mongodb type: standalone, mongos or repl.
    """
    type = ''
    try:
        mc = connect(host, port, **kwargs)
        if mc.is_mongos:
            type = 'mongos'
        else:
            status = mc.admin.command({'replSetGetStatus': 1})
            if status['ok'] == 1:
                type = 'repl'
    except pymongo.errors.OperationFailure as e:
        type = 'standalone'
        mc.close()
    except pymongo.errors.ConnectionFailure as e:
        print 'connect to %s:%d failed: %s' % (host, port, e)
    return type


def get_optime(repl_optime_item):
    """ Get optime in seconds.
    Compatible for version 0 and 1 of the replication protocol.
    By default, new replica sets in MongoDB 3.2 use protocolVersion: 1.
    Previous versions of MongoDB use version 0 of the protocol.
    Refer to https://docs.mongodb.com/manual/reference/replica-configuration/#rsconf.protocolVersion
    """
    if isinstance(repl_optime_item, dict):
        if 'ts' in repl_optime_item:
            return repl_optime_item['ts'].time
    else:
        return repl_optime_item.time
    return None


class Stats(object):
    """ Stats of mongod instance.
    """
    def __init__(self, host, port):
        self.hostportstr = '%s:%d' % (host, port)
        self.host = host
        self.port = port
        self.engine = 'n/a'
        self.replname = 'n/a'
        self.state = 'n/a'     # should query on primary
        self.stateStr = 'n/a'  # should query on primary
        self.syncingTo = 'n/a' # should query on primary
        self.syncDelay = 'n/a' # should query on primary
        self.conn = 'n/a'
        self.qr = 'n/a' 
        self.qw = 'n/a'
        self.ar = 'n/a'
        self.aw = 'n/a'
        self.cache_size = 'n/a'
        self.cache_used = 'n/a'
        self.res = 'n/a'
        self.uptime = 'n/a'
        self.version = 'n/a'


class OutputFormatter:
    """ Relate to class 'Stats'.
    """
    def __init__(self):
        self.__key_order = ['repl', 'host', 'state', 'conn', 'qr', 'qw', 'ar', 'aw', 'size', 'used', 'res', 'syncDelay', 'uptime', 'engine', 'version']
        self.__padding = 2
        self.__attribute_name_map = {
            'host': 'hostportstr',
            'repl': 'replname',
            'size': 'cache_size',
            'used': 'cache_used',
            'state': 'stateStr'
        }

    def __calc_rjust(self, key):
        padding = 0 if key == self.__key_order[0] else self.__padding
        if isinstance(self.__decorated_stats, Stats):
            rjust = max(len(key), self.__strlen((getattr(self.__decorated_stats, self.__get_attribute_name(key))))) + padding
        elif isinstance(self.__decorated_stats, list):
            lens = [ self.__strlen((getattr(item, self.__get_attribute_name(key)))) for item in self.__decorated_stats ]
            lens.append(len(key))
            rjust = max(lens) + padding
        return rjust

    def __get_rjust(self, key):
        return self.__rjust_dict[key]

    def __width(self):
        return sum([ val for _, val in self.__rjust_dict.iteritems() ])

    def __get_attribute_name(self, key):
        return self.__attribute_name_map.get(key, key)

    def __format(self, stats):
        strs = []
        for key in self.__key_order:
            val = getattr(stats, self.__get_attribute_name(key))
            if isinstance(val, ColorString):
                strs.append(val.rjust(self.__get_rjust(key)))
            else:
                strs.append(str(val).rjust(self.__get_rjust(key)))
        return ''.join(strs)

    def __strlen(self, val):
        if isinstance(val, str):
            return len(val)
        elif isinstance(val, ColorString):
            return val.display_len()
        else:
            return len(str(val))

    def __decorate(self, stats):
        """ Reset unit and highlight the output here.
        """
        if stats.qr != 'n/a':
            if stats.qr >= 1000:
                stats.qr = ColorString(1, 31, 49, '%d' % stats.qr)
            elif stats.qr >= 100:
                stats.qr = ColorString(1, 33, 49, '%d' % stats.qr)
            else:
                stats.qr = '%d' % stats.qr

        if stats.qw != 'n/a':
            if stats.qw >= 1000:
                stats.qw = ColorString(1, 31, 49, '%d' % stats.qw)
            elif stats.qw >= 100:
                stats.qw = ColorString(1, 33, 49, '%d' % stats.qw)
            else:
                stats.qw = '%d' % stats.qw

        if stats.cache_used != 'n/a':
            used = round(stats.cache_used*100, 1)
            if used >= 90.0:
                stats.cache_used = ColorString(1, 31, 49, '%.1f%%' % used)
            elif used > 80.0:
                stats.cache_used = ColorString(1, 33, 49, '%.1f%%' % used)
            else:
                stats.cache_used = '%.1f%%' % used

        if stats.cache_size != 'n/a':
            stats.cache_size = '%dG' % (stats.cache_size / 1024 / 1024 / 1024)

        if stats.res != 'n/a':
            stats.res = '%.1fG' % (float(stats.res) / 1024)

        if stats.uptime != 'n/a':
            stats.uptime = '%.1fh' % (float(stats.uptime) / 3600)

        return stats

    def output(self, stats):
        if isinstance(stats, Stats):
            self.__decorated_stats = self.__decorate(stats)
        elif isinstance(stats, list):
            self.__decorated_stats = [ self.__decorate(item) for item in stats ]
        else:
            raise RuntimeError('invalid stats type')

        self.__rjust_dict = { key: self.__calc_rjust(key) for key in self.__key_order }

        column_names = [ key.rjust(self.__get_rjust(key)) for key in self.__key_order ]
        print ''
        print ''.join(column_names)
        print '-' * self.__width()
        if isinstance(stats, Stats):
            print self.__format(stats)
        elif isinstance(stats, list):
            for item in stats:
                print self.__format(item)


def repl_node_cmp(s1, s2):
    """ Custom compare function between replica set members.
    Sort by replica set member state.
    Refers to https://docs.mongodb.org/manual/reference/replica-states/
    """
    if s1.state < s2.state:
        return -1
    elif s1.state == s2.state:
        return 0
    else:
        return 1


def handle_node(host, port, **kwargs):
    s = Stats(host, port) 
    try:
        mc = connect(host, port, **kwargs)
        res = mc.admin.command({'serverStatus': 1})
        mc.close()
        s.hostportstr = '%s:%d' % (host, port)
        s.host = host
        s.port = port
        s.version = res['version']
        s.conn = res['connections']['current']
        s.res = res['mem']['resident']
        s.uptime = res['uptime']
        if 'storageEngine' in res:
            s.engine = res['storageEngine']['name']
        else:
            s.engine = 'mmapv1'
        if s.engine == 'wiredTiger':
            s.qr = res['globalLock']['currentQueue']['readers'] + res['globalLock']['activeClients']['readers'] - res['wiredTiger']['concurrentTransactions']['read']['out']
            if s.qr < 0:
                s.qr = 0
            s.qw = res['globalLock']['currentQueue']['writers'] + res['globalLock']['activeClients']['writers'] - res['wiredTiger']['concurrentTransactions']['write']['out']
            if s.qw < 0:
                s.qw = 0
            s.ar = res['wiredTiger']['concurrentTransactions']['read']['out']
            s.aw = res['wiredTiger']['concurrentTransactions']['write']['out']
            if res['wiredTiger']['cache']['maximum bytes configured'] != 0:
                s.cache_size = res['wiredTiger']['cache']['maximum bytes configured']
                s.cache_used = res['wiredTiger']['cache']['bytes currently in the cache'] / s.cache_size
                #s.cache_dirty = res['wiredTiger']['cache']['tracked dirty bytes in the cache'] / res['wiredTiger']['cache']['maximum bytes configured']
        elif s.engine == 'mmapv1': # mmapv1
            s.qr = res['globalLock']['currentQueue']['readers']
            s.qw = res['globalLock']['currentQueue']['writers']
            s.ar = res['globalLock']['activeClients']['readers']
            s.aw = res['globalLock']['activeClients']['writers']
        else:
            raise RuntimeError('engine not supported: %s' % s.engine)
    except Exception as e:
        errstr = 'failed to check %s:%d: %s' % (host, port, e)
        errors.append(errstr)
    return s


def handle_standalone(host, port, **kwargs):
    stats = handle_node(host, port, **kwargs)
    formatter = OutputFormatter()
    formatter.output(stats)


def handle_repl(host, port, **kwargs):
    try:
        mc = connect(host, port, **kwargs)
        res = mc.admin.command({'isMaster': 1})
        mc.close()
        members = []
        if 'hosts' in res:
            members.extend(res['hosts'])
        if 'passives' in res:
            members.extend(res['passives'])
        if 'arbiters' in res:
            members.extend(res['arbiters'])

        member_stats = {}
        for hostportstr in members:
            host, port = parse_hostportstr(hostportstr)
            member_stats[hostportstr] = handle_node(host, port, **kwargs)
        
        if 'primary' in res:
            primary = res['primary']
            primary_host, primary_port = parse_hostportstr(primary)
            mc = connect(primary_host, primary_port, **kwargs)
            res = mc.admin.command({'replSetGetStatus': 1})
            mc.close()

            replname = res['set']
            pri_optime = None

            # get primary optime
            for member in res['members']:
                if member['stateStr'] == 'PRIMARY':
                    pri_optime = get_optime(member['optime'])
                    break

            for member in res['members']:
                hostportstr = member['name']
                if hostportstr not in member_stats:
                    # TODO 
                    # hiden member not in node list
                    continue
                member_stats[hostportstr].replname = replname
                member_stats[hostportstr].state = member['state']
                member_stats[hostportstr].stateStr = member['stateStr']
                if 'syncingTo' in member:
                    member_stats[hostportstr].syncingTo = member['syncingTo']
                if 'optime' in member:
                    member_stats[hostportstr].syncDelay = '%ds' % (pri_optime - get_optime(member['optime']))

        stats_list = [ stats for stats in member_stats.itervalues() ]
        stats_list.sort(repl_node_cmp)
        formatter = OutputFormatter()
        formatter.output(stats_list)
    except Exception as e:
        print 'handle repl failed: %s' % e


def replstr_to_nodes(replstr):
    """ replstr LIKE 'replname/host:port[,host:port...]'
    """
    hostportstrs = replstr.split('/')[1]
    return hostportstrs.split(',')


def handle_shard(replstr, **kwargs):
    """ replstr LIKE 'replname/host:port[,host:port...]'
    """
    nodes = replstr_to_nodes(replstr)
    for node in nodes:
        host, port = parse_hostportstr(node)
        handle_repl(host, port, **kwargs)
        break
            

def handle_mongos(host, port, **kwargs):
    try:
        mc = connect(host, port, **kwargs)
        res = mc.admin.command({'listShards': 1})
        mc.close()
    except Exception as e:
        print 'command listShards failed on %s:%d: %s' % (host, port, e)

    for shard in res['shards']:
        handle_shard(shard['host'], **kwargs)


class ColorString(object):
    """ String with specified format, foreground and background colors.

    Formatting:
    Set:
        1 => bold/bright
        2 => dim
        4 => underlined
        5 => blink
        7 => reverse (invert the foreground and background colors)
        8 => hidden (usefull for passwords)
    Reset:
        0 => reset all attributes
        21 => reset bold/bright
        22 => reset dim
        24 => reset underlined
        25 => reset blink
        27 => reset reverse (invert the foreground and background colors)
        28 => reset hidden (usefull for passwords)

    Foreground:
        30 => black
        31 => red
        32 => green
        33 => yellow
        34 => blue  
        35 => magenta
        36 => cyan
        37 => light gray
        39 => default foreground color
        90 => dark gray
        91 => light red
        92 => light green
        93 => light yellow
        94 => light blue  
        95 => light magenta
        96 => light cyan
        97 => white

    Background:
        40 => black
        41 => red
        42 => green
        43 => yellow
        44 => blue  
        45 => magenta
        46 => cyan
        47 => light gray
        49 => default background color
        100 => dark gray
        101 => light red
        102 => light green
        103 => light yellow
        104 => light blue  
        105 => light magenta
        106 => light cyan
        107 => white
    """
    def __init__(self, format, foreground, background, s):
        if not isinstance(format, int):
            raise Exception('TypeError: ColorString() argument 1 must be integer')
        if not isinstance(foreground, int):
            raise Exception('TypeError: ColorString() argument 2 must be integer')
        if not isinstance(background, int):
            raise Exception('TypeError: ColorString() argument 3 must be integer')
        if not isinstance(s, str):
            raise Exception('TypeError: ColorString() argument 4 must be str')
        self.s = s
        self.prefix = '\033[%d;%d;%dm' % (format, foreground, background)
        self.suffix = '\033[0m'

    def __str__(self):
        return self.prefix + self.s + self.suffix

    def display_len(self):
        return len(self.s)

    def storage_len(self):
        return len(self.prefix + self.s + self.suffix)

    def ljust(self, width):
        n = width - len(self.s)
        if n < 0:
            n = 0
        return self.prefix + self.s + self.suffix + ' '*n

    def rjust(self, width):
        n = width - len(self.s)
        if n < 0:
            n = 0
        return ' '*n + self.prefix + self.s + self.suffix


if __name__ == '__main__':
    parse_arguments()

    print 'pymongo version:', pymongo.version
    print 'host:', g_host
    print 'port:', g_port
    print 'username:', g_username
    print 'password:', g_password

    errors = []

    type = get_mongo_type(g_host, g_port, username=g_username, password=g_password)
    if type == 'repl':
        handle_repl(g_host, g_port, username=g_username, password=g_password)
    elif type == 'mongos':
        handle_mongos(g_host, g_port, username=g_username, password=g_password)
    elif type == 'standalone':
        handle_standalone(g_host, g_port, username=g_username, password=g_password)
    else:
        raise RuntimeError('invalid mongo type: %s' % type)

    if errors:
        print '\nerrors:'
        for err in errors:
            print err
