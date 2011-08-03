#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4

# loosely based on Twisted Matrix Laboratories examples

from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor

config = { 'hosts': 
        [
            { 'hostname': 'amy.in.lee-loo.net', 'port': '4949' },
            { 'hostname': 'shawan.in.lee-loo.net', 'port': '4949' }
        ]
        }

class MuninRelay(LineReceiver):
    _init_done = False

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

