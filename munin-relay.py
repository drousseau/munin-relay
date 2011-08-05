#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4

# loosely based on Twisted Matrix Laboratories examples

import re
import time
import md5
from twisted.internet.protocol import Factory
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor

config = { 'hosts': 
        [
            { 'hostname': 'amy.in.lee-loo.net', 'hostaddr': 'amy.in.lee-loo.net', 'port': '4949' },
            { 'hostname': 'shawan.in.lee-loo.net', 'hostaddr': '10.2.5.2', 'port': '4949' }
        ]
        }

class MuninClient(LineReceiver):
    _host = None
    _f_parent = None

    def connectionMade(self):
        print "new connect : ", dir(self)
        print dir(self.transport)
        print "addr:", self.transport.addr
        client = self.transport.addr
        self._f_parent = self.factory._parent
        for h in self._f_parent.factory.cfg['hosts']:
            if ( (h['hostaddr'] == client[0]) and (int(h['port']) == client[1]) ):
                self._host = h
                self._f_parent._clients[h['hostname']] = self

    def connectionLost(self, reason):
        print "lost connect : ", dir(self), reason
        client = self.transport.addr
        del self._f_parent._clients[self._host['hostname']]

    def dataReceived(self, data):
        print "client data receive : ", data
        for l in (data.split("\n")):
            self._f_parent._handle_line(self._host, l)

class MuninClientFactory(ClientFactory):
    protocol = MuninClient
    _parent = None

    def clientConnectionFailed(self, connector, reason):
        print 'connection failed:', reason.getErrorMessage()
        print dir(connector)

class MuninRelay(LineReceiver):
    _re_list = re.compile("^list +(.+) *$", re.I)
    _re_fetch = re.compile("^fetch +([0-9a-f]{32})_(.+) *$", re.I)
    _re_config = re.compile("^config +([0-9a-f]{32})_(.+) *$", re.I)
    _re_comment = re.compile("^#")
    _clients = {}
    _c_factory = None
    _last_command = {}
    _host2hash = {}
    _hash2host = {}

    def _get_host_from_hash(self, h):
        hostname = ''
        if (self._hash2host.has_key(h)):
            hostname = self._hash2host[h]
        else:
            for host in self.factory.cfg['hosts']:
                print "h2h : ", host['hostname']
                m = md5.new(host['hostname'])
                hash_v = m.hexdigest()
                del m
                if (hash_v == h):
                    hostname = host['hostname']
                    self._hash2host[hash_v] = hostname
                    self._host2hash[hostname] = hash_v
        return hostname
        
    def _open_host(self, host):
        print "_open_host:", host, self._clients
        if (self._clients.has_key(host)):
            return
        if (self._c_factory == None):
            self._c_factory = MuninClientFactory()
            self._c_factory._parent = self
        for h in self.factory.cfg['hosts']:
            if (h['hostname'] == host):
                self._last_command[h['hostname']] = None
                m = md5.new(h['hostname'])
                hash_v = m.hexdigest()
                del m
                self._hash2host[hash_v] = h['hostname']
                self._host2hash[h['hostname']] = hash_v
                reactor.connectTCP(h['hostaddr'], int(h['port']), self._c_factory)

    def _send_line(self, hostname, line, arg=''):
        self._last_command[hostname] = line
        if (arg != ''):
            line = line + ' ' + arg
        print "send line : '" + line + "' to " + hostname
        self._clients[hostname].sendLine(line)
        
    def _handle_line(self, host, line):
        line = line.rstrip()

        if (line == ''):
            return

        m_comment = self._re_comment.match(line)
        if (m_comment != None):
            return

        hostname = host['hostname']

        if (self._last_command[hostname] == 'list'):
            self._last_command[hostname] = None
            retline = ''
            hash_v = self._host2hash[hostname]
            for plugin in line.split(' '):
                retline += hash_v + '_' + plugin + ' '
            self.sendLine(retline)
            return
            
        if (self._last_command[hostname] == 'fetch'):
            if (line == '.'):
                print "end of fetch"
                self._last_command[hostname] = None
            self.sendLine(line)
            return
            
        if (self._last_command[hostname] == 'config'):
            if (line == '.'):
                print "end of config"
                self._last_command[hostname] = None
            self.sendLine(line)
            return
            

        self.sendLine("line recevied for command '" + self._last_command[hostname] + "' from " + host['hostname'] + " : " + line)

    def connectionMade(self):
        """
        Handle some ACL checks, and log connection
        """
        # print dir(self.transport)
        print "Connection received from : ", self.transport.client[0]
        self.sendLine("# welcome to munin-relay")

    def lineReceived(self, data):
        """
        As soon as any data is received, write it back.
        """
        #self.transport.write(data)
        print "received : ", data
        if (data == 'nodes'):
            for h in self.factory.cfg['hosts']:
                self.sendLine(h['hostname'])
            self.sendLine('.')
        elif (data == 'quit'):
            self.transport.loseConnection()
        else:
            m_list = self._re_list.match(data)
            m_fetch = self._re_fetch.match(data)
            m_config = self._re_config.match(data)
            print m_list, m_fetch, m_config
            if (m_list != None):
                hostname = m_list.group(1)
                if ( not (self._clients.has_key(hostname)) ):
                    self._open_host(hostname)
                reactor.callLater (0.1, self._send_line, hostname, 'list')
                self.sendLine("# fetch list for " + m_list.group(1))

            if (m_fetch != None):
                hosthash = m_fetch.group(1)
                plugin = m_fetch.group(2)
                print "fetch " + hosthash + " " + plugin
                hostname = self._get_host_from_hash(hosthash)
                if (hostname != ''):
                    if ( not (self._clients.has_key(hostname)) ):
                        self._open_host(hostname)
                    reactor.callLater (0.1, self._send_line, hostname, 'fetch', plugin)
                    self.sendLine("# fetch plugin " + plugin + " for " + hostname)

            if (m_config != None):
                hosthash = m_config.group(1)
                plugin = m_config.group(2)
                print "config " + hosthash + " " + plugin
                hostname = self._get_host_from_hash(hosthash)
                if (hostname != ''):
                    if ( not (self._clients.has_key(hostname)) ):
                        self._open_host(hostname)
                    reactor.callLater (0.1, self._send_line, hostname, 'config', plugin)
                    self.sendLine("# config plugin " + plugin + " for " + hostname)


class MuninRelayFactory(Factory):
    protocol = MuninRelay

    def __init__(self, config):
        self.cfg = config
            

def main():
    f = MuninRelayFactory(config)
    reactor.listenTCP(4950, f)
    reactor.run()

if __name__ == '__main__':
    main()

