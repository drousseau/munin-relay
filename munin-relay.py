#!/usr/bin/python3
# vim: set tabstop=4 expandtab shiftwidth=4:

# loosely based on Twisted Matrix Laboratories examples


from __future__ import print_function
from future import standard_library

standard_library.install_aliases()
from builtins import str
from future.utils import raise_
import os, sys
import time
import syslog
import re
import hashlib
import configparser
import argparse
from twisted.internet.protocol import Factory
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet.task import LoopingCall
from twisted.internet import reactor
from configparser import ConfigParser

# Default conf
config = {
    'hosts': [],
    'allowed_ip': ['127.0.0.1'],
    'port': '4950',
    'bind_address': '127.0.0.1',
}


class MuninClient(LineReceiver):
    _host = None
    _f_parent = None
    _last_activity = 0

    def connectionMade(self):
        self.delimiter = bytes("\n", 'utf-8')
        print("new connect : ", dir(self))
        print(dir(self.transport))
        print("addr:", self.transport.addr)
        client = self.transport.addr
        self._f_parent = self.factory._parent
        self._last_activity = time.time()
        for h in self._f_parent.factory.cfg['hosts']:
            if (h['hostaddr'] == client[0]) and (int(h['port']) == client[1]):
                self._host = h
                self._f_parent._clients[h['hostname']] = self

    def connectionLost(self, reason):
        print("lost connect : ", dir(self), reason)
        client = self.transport.addr
        del self._f_parent._clients[self._host['hostname']]

    def lineReceived(self, l):
        self._last_activity = time.time()
        print("client data receive : ", l)
        l = l.replace(bytes("\r", 'utf-8'), bytes("", 'utf-8'))
        self._f_parent._handle_line(self._host, l.decode('utf-8'))

    def _disconnect(self):
        self.transport.loseConnection()


class MuninClientFactory(ClientFactory):
    protocol = MuninClient
    _parent = None

    def clientConnectionFailed(self, connector, reason):
        print('connection failed:', reason.getErrorMessage())
        print(dir(connector))


class MuninRelay(LineReceiver):
    _re_list = re.compile("^list +(.+) *$", re.I)
    _re_fetch = re.compile("^fetch +([0-9a-f]{32})_(.+) *$", re.I)
    _re_config = re.compile("^config +([0-9a-f]{32})_(.+) *$", re.I)
    _re_cap = re.compile("^cap +.+$", re.I)
    _re_cap_names = re.compile(" +(\w+)", re.I)
    _re_comment = re.compile("^#")
    _re_greeting = re.compile("^# (lrrd|munin) (.+) (on|at) (.+)$", re.I)

    _clients = {}
    _c_factory = None
    _last_command = {}
    _last_command_for = {}
    _host2hash = {}
    _hash2host = {}
    _cap_multigraph = False
    _help_line = "# Allowed commands: nodes, list, config, fetch, version or quit"

    def sendBytesLine(self, line):
        return self.transport.write(bytes(line + str(self.delimiter), 'utf-8'))

    def _get_host_from_hash(self, h):
        hostname = ''
        if (h in self._hash2host):
            hostname = self._hash2host[h]
        else:
            for host in self.factory.cfg['hosts']:
                print("h2h : ", host['hostname'])
                m = hashlib.md5(host['hostname'].encode('utf-8'))
                hash_v = m.hexdigest()
                del m
                if (hash_v == h):
                    hostname = host['hostname']
                    self._hash2host[hash_v] = hostname
                    self._host2hash[hostname] = hash_v
        return hostname

    def _open_host(self, host):
        print("_open_host:", host, self._clients)
        if (host in self._clients):
            return
        if (self._c_factory == None):
            self._c_factory = MuninClientFactory()
            self._c_factory._parent = self
        for h in self.factory.cfg['hosts']:
            if (h['hostname'] == host):
                self._last_command[h['hostname']] = None
                self._last_command_for[h['hostname']] = None
                m = hashlib.md5(h['hostname'].encode('utf-8'))
                hash_v = m.hexdigest()
                del m
                self._hash2host[hash_v] = h['hostname']
                self._host2hash[h['hostname']] = hash_v
                reactor.connectTCP(h['hostaddr'], int(h['port']), self._c_factory)

    def _send_line(self, hostname, line, arg=''):
        self._last_command[hostname] = line
        self._last_command_for[hostname] = self
        if (arg != ''):
            line = line + ' ' + arg
        print("send line : '" + line + "' to " + hostname)
        if (hostname not in self._clients):
            self.sendBytesLine('# error: unknow host ' + hostname)
            return
        self._clients[hostname].transport.write(bytes(line + self.delimiter, 'utf-8'))

    def _handle_line(self, host, line):
        line = line.rstrip()
        hostname = host['hostname']
        # The response beeing read can be for a client different of the "parent" of the object
        client = self._last_command_for[hostname]
        if (client == None):
            client = self
        print("handle line '" + line + "' for " + hostname, "client is", client._client)
        print("Last command for", hostname, "is", self._last_command[hostname])

        if (line == ''):
            print("empty line, do nothing - handle line")
            return

        m_greeting = self._re_greeting.match(line)
        if (m_greeting != None):
            if (self._cap_multigraph):
                # announce cap multigraph, if we received it from server
                self._clients[hostname].sendBytesLine("cap multigraph")
            return
        m_comment = self._re_comment.match(line)
        if (m_comment != None):
            return

        # ignore reply for cap
        m_cap = self._re_cap.match(line)
        if (m_cap != None):
            return

        if (self._last_command[hostname] == 'list'):
            self._last_command[hostname] = None
            self._last_command_for[hostname] = None
            retline = ''
            hash_v = self._host2hash[hostname]
            for plugin in line.split(' '):
                retline += hash_v + '_' + plugin + ' '
            print("list for", hostname, "result:", retline + '<<')
            # self.sendLine(retline)
            client.sendBytesLine(retline)
            return

        if (self._last_command[hostname] == 'fetch'):
            if (line == '.'):
                print("end of fetch")
                self._last_command[hostname] = None
                self._last_command_for[hostname] = None
            # self.sendLine(line)
            client.sendBytesLine(line)
            return

        if self._last_command[hostname] == 'config':
            if line == '.':
                print("end of config")
                self._last_command[hostname] = None
                self._last_command_for[hostname] = None
            print("sending '" + line + "'")
            # self.sendLine(line)
            client.sendBytesLine(line)
            return

        self.sendBytesLine(
            "line received for command '" + self._last_command[hostname] + "' from " + host['hostname'] + " : " + line)

    def connectionMade(self):
        """
        Handle some ACL checks, and log connection
        """
        # print dir(self.transport)
        print("Connection received from : ", self.transport.client[0])
        self._client = self.transport.client
        self.factory._client_connected(self)
        if (not self.transport.client[0] in self.factory.cfg['allowed_ip']):
            self.sendBytesLine(str("# ") + str(self.transport.client[0]) + str(" is not allowed"))
            self.transport.loseConnection()

        self.delimiter = "\x0a"
        self.sendBytesLine("# munin node at munin-relay" + self.delimiter)

    def connectionLost(self, reason):
        self.factory._client_disconnected(self)

    def _do_line(self, data):
        data=data.decode('utf-8')
        if (data == 'nodes'):
            for h in self.factory.cfg['hosts']:
                self.sendBytesLine(h['hostname'])
            self.sendBytesLine('.')
        elif (data == 'version'):
            self.sendBytesLine("# munin-relay in development")
        elif (data == 'list'):
            self.sendBytesLine("# missing node name after list.")
            self.sendBytesLine("# Usage: list node_name")
        elif (data == 'quit'):
            self.transport.loseConnection()
        elif (data == ''):
            print("empty line, do nothing - do line")
        else:
            m_list = self._re_list.match(data)
            m_fetch = self._re_fetch.match(data)
            m_config = self._re_config.match(data)
            m_cap = self._re_cap.match(data)
            print(m_list, m_fetch, m_config, m_cap)
            done = False
            if (m_list != None):
                hostname = m_list.group(1)

                if (not (hostname in self._clients)):  # Socket exist ?
                    self._open_host(hostname)
                reactor.callLater(0.1, self._send_line, hostname, 'list')
                #                self.sendLine("# fetch list for " + m_list.group(1))
                done = True

            if (m_cap != None):
                cap_names = self._re_cap_names.findall(data)
                for cap_name in cap_names:
                    print("cap : '" + cap_name + "'")
                    if (cap_name == 'multigraph'):
                        self._cap_multigraph = True
                        self.sendBytesLine("cap multigraph")
                done = True

            if (m_fetch != None):
                hosthash = m_fetch.group(1)
                plugin = m_fetch.group(2)
                print("fetch " + hosthash + " " + plugin)
                hostname = self._get_host_from_hash(hosthash)
                if (hostname != ''):
                    if (not (hostname in self._clients)):
                        self._open_host(hostname)
                    reactor.callLater(0.1, self._send_line, hostname, 'fetch', plugin)
                #                    self.sendLine("# fetch plugin " + plugin + " for " + hostname)
                done = True

            if (m_config != None):
                hosthash = m_config.group(1)
                plugin = m_config.group(2)
                print("config " + hosthash + " " + plugin)
                hostname = self._get_host_from_hash(hosthash)
                if (hostname != ''):
                    if (not (hostname in self._clients)):
                        self._open_host(hostname)
                    reactor.callLater(0.1, self._send_line, hostname, 'config', plugin)
                #                    self.sendLine("# config plugin " + plugin + " for " + hostname)
                done = True

            if (not done):
                self.sendBytesLine(self._help_line)
                print("unrecognized command received : ", data)

    def dataReceived(self, data):
        print("received : ", data)
        data = data.replace(bytes("\r\n", 'utf-8'), bytes("\n", 'utf-8'))
        data = data.replace(bytes("\r", 'utf-8'), bytes("\n", 'utf-8'))
        data = data.replace(bytes("\n\n", 'utf-8'), bytes("\n", 'utf-8'))
        # [:-1] because the split gives an empty value after the final \n from command list
        for l in data.split(bytes("\n", 'utf-8'))[:-1]:
            if (l == ''):  # Empty line
                self.sendBytesLine(self._help_line)
                break
            self._do_line(l)


class MuninRelayFactory(Factory):
    protocol = MuninRelay

    def __init__(self, config):
        self.cfg = config
        self._instances = []

    def _client_connected(self, instance):
        print("One client connected", instance)
        self._instances.append(instance)

    def _client_disconnected(self, instance):
        print("One client disconnected", instance)
        try:
            self._instances.remove(instance)
        except:
            pass

    def clean_old_cnx(self):
        print("Clean old connections")
        now = time.time()
        for i in self._instances:
            for k in list(i._clients.keys()):
                c = i._clients[k]
                # print c._last_activity
                # inactivity 10s
                if ((c._last_activity + 10) < now):
                    print(k, "looks old")
                    c._disconnect()


def old_connections_callback(fact):
    fact.clean_old_cnx()


def main():
    f = MuninRelayFactory(config)
    if ('port' not in config or (config['port'] == '')):
        print("Missing port parameter in global config")
        sys.exit(-1)
    if ('bind_address' not in config or (config['bind_address'] == '')):
        print("Missing bind_address parameter in global config")
        sys.exit(-1)
    if (config['debug']):
        print("Listening on %(bind_address)s:%(port)s" % config)
    reactor.listenTCP(int(config['port']), f, 50, config['bind_address'])
    if (args.pid_file != ''):
        try:
            ff = open(args.pid_file, "w")
            ff.write(str(os.getpid()))
            ff.close()
        except Exception as e:
            print("Error writing in %s :" % args.pid_file, e)
    lc = LoopingCall(old_connections_callback, (f))
    lc.start(1)
    reactor.run()


parser = argparse.ArgumentParser(description='Munin relay.')
parser.add_argument('--debug', action='store_true', default=False, dest='debugme', help='Do not detach process')
parser.add_argument('--pid', action='store', default='', dest='pid_file', help='Specify a PID file')
parser.add_argument('--config', action='store', default=None, dest='configfile', help='Path to configuration file')

args = parser.parse_args()

# Read the configuration file
ccfg = ConfigParser()
if (args.configfile != None):
    configpaths = [args.configfile]
else:
    # Defaults locations
    configpaths = ['munin-relay.ini', '/etc/munin/munin-relay.ini']
if (args.debugme):
    print("Reading configurations from", configpaths)
ccfg.read(configpaths)

for section_name in ccfg.sections():
    if (section_name == 'global'):
        for i in ['allowed_ip', 'port', 'bind_address']:
            if i in ccfg.options(section_name):
                if (i in ['allowed_ip']):
                    config[i] = ccfg.get('global', i).split(',')
                else:
                    config[i] = ccfg.get('global', i)
    else:
        tmp = {'port': '4949'}
        for op in ccfg.options(section_name):
            tmp[op] = ccfg.get(section_name, op)
        config['hosts'].append(tmp)

config['debug'] = args.debugme

if ('allowed_ip' not in config or len(config['allowed_ip']) == 0):
    print("Problem with configuration files, no one is allowed")
    sys.exit()

# Now the configuration is in config 
# print repr(config)
# sys.exit()


# Background process
if (args.debugme == True):
    print("Debug mode: do not detach")
    main()
    sys.exit(0)
else:
    try:
        try:
            pid = os.fork()
        except OSError as e:
            syslog.syslog("Failed to daemonize")
            raise_(Exception, "%s [%d]" % (e.strerror, e.errno))

        orig_stderr = sys.stderr
        if (pid == 0):  # the daemon child
            try:
                os.setsid()  # set own session
                os.chdir("/tmp")
                null = open('/dev/null', 'a')
                sys.stdout = null
                sys.stderr = null
                main()
            except Exception as e:
                print("Error: failed to launch munin-relay :", e, file=orig_stderr)
                sys.exit(2)
        else:  # the parent
            syslog.syslog("Launch munin-relay #" + str(pid))
            sys.exit(0)

    except KeyboardInterrupt:
        print("Interrupt")
        sys.exit(0)
