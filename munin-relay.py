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
        self._f_parent._handle_line(self._host, data)

class MuninClientFactory(ClientFactory):
    protocol = MuninClient
    _parent = None

    def clientConnectionFailed(self, connector, reason):
        print 'connection failed:', reason.getErrorMessage()
        print dir(connector)

class MuninRelay(LineReceiver):
    _re_list = re.compile("^list +(.+)$", re.I)
    _re_comment = re.compile("^#")
    _clients = {}
    _c_factory = None
    _last_command = {}
    _host2hash = {}
    _hash2host = {}

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

    def _send_line(self, hostname, line):
        self._last_command[hostname] = line
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
        else:
            m_list = self._re_list.match(data)
            if (m_list != None):
                hostname = m_list.group(1)
                if ( not (self._clients.has_key(hostname)) ):
                    self._open_host(hostname)
                reactor.callLater (0.1, self._send_line, hostname, 'list')
                self.sendLine("fetch list for " + m_list.group(1))


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

