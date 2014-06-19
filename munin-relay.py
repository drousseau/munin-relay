#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4

# loosely based on Twisted Matrix Laboratories examples


import os, sys
import time
import syslog
import re
import md5
import ConfigParser
import argparse
from twisted.internet.protocol import Factory
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from ConfigParser import SafeConfigParser

# Default conf
config = {
  'hosts': [],
  'allowed_ip': ['127.0.0.1'],
  'port': '4950',
  'bind_address': '127.0.0.1',
 }

# Read the configuration file
ccfg = SafeConfigParser()
ccfg.read([ 'munin-relay.ini', '/etc/munin/munin-relay.ini'])
for section_name in ccfg.sections():
  if ( section_name == 'global' ):
    for i in ['allowed_ip', 'port', 'bind_address']:
      if i in ccfg.options(section_name):
        if ( i in ['allowed_ip'] ):
          config[i] = ccfg.get('global', i).split(',')
        else:
          config[i] = ccfg.get('global', i)
  else:
    tmp = { 'port': '4949' }
    for op in ccfg.options(section_name):
      tmp[op]=ccfg.get(section_name,op)
    config['hosts'].append( tmp )

if ( not config.has_key('allowed_ip') or len(config['allowed_ip']) == 0 ):
  print "Problem with configuration files, no one is allowed"
  sys.exit()

# Now the configuration is in config 
# print repr(config)
# sys.exit()

class MuninClient(LineReceiver):
    _host = None
    _f_parent = None

    def connectionMade(self):
        self.delimiter = "\n"
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

    def lineReceived(self, l):
        print "client data receive : ", l
        l = l.replace("\r", "")
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
    _re_cap = re.compile("^cap +(.+) *$", re.I)
    _re_comment = re.compile("^#")
    _re_greeting = re.compile("^# (lrrd|munin) (.+) on (.+)$", re.I)

    _clients = {}
    _c_factory = None
    _last_command = {}
    _host2hash = {}
    _hash2host = {}
    _cap_multigraph = False
    _help_line = "# Allowed commands: nodes, list, config, fetch, version or quit"

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
        if ( not self._clients.has_key(hostname)):
          self.sendLine('# error: unknow host ' + hostname)
          return
        self._clients[hostname].sendLine(line)
        
    def _handle_line(self, host, line):
        line = line.rstrip()
        hostname = host['hostname']
        print "handle line '" + line + "' for " + hostname

        if (line == ''):
            print "empty line, do nothing - handle line"
            return

        m_greeting = self._re_greeting.match(line)
        if (m_greeting != None):
            if (self._cap_multigraph):
                # announce cap multigraph, if we received it from server
                self._clients[hostname].snedLine("cap multigraph")
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
            print "sending '" + line + "'"
            self.sendLine(line)
            return
            

        self.sendLine("line received for command '" + self._last_command[hostname] + "' from " + host['hostname'] + " : " + line)

    def connectionMade(self):
        """
        Handle some ACL checks, and log connection
        """
        # print dir(self.transport)
        print "Connection received from : ", self.transport.client[0]
        if ( not self.transport.client[0] in self.factory.cfg['allowed_ip'] ):
          self.sendLine("# " + self.transport.client[0] + " is not allowed")
          self.transport.loseConnection()

        self.delimiter = "\x0a"
        self.sendLine("# munin node at munin-relay")

    def _do_line(self, data):
        if (data == 'nodes'):
            for h in self.factory.cfg['hosts']:
                self.sendLine(h['hostname'])
            self.sendLine('.')
        elif (data == 'version'):
            self.sendLine("# munin-relay in development")
        elif (data == 'list'):
            self.sendLine("# missing node name after list.")
            self.sendLine("# Usage: list node_name")
        elif (data == 'quit'):
            self.transport.loseConnection()
        elif (data == ''):
            print "empty line, do nothing - do line"
        else:
            m_list = self._re_list.match(data)
            m_fetch = self._re_fetch.match(data)
            m_config = self._re_config.match(data)
            m_cap = self._re_cap.match(data)
            print m_list, m_fetch, m_config
            if (m_list != None):
                hostname = m_list.group(1)
                

                if ( not (self._clients.has_key(hostname)) ): # Socket exist ?
                    self._open_host(hostname)
                reactor.callLater (0.1, self._send_line, hostname, 'list')
#                self.sendLine("# fetch list for " + m_list.group(1))

            if (m_cap != None):
                cap_name = m_cap.group(1)
                print "cap : '" + cap_name + "'"
                if (cap_name == 'multigraph'):
                    self._cap_multigraph = True
                    self.sendLine("cap multigraph")
                else:
                    self.sendLine(self._help_line)

            if (m_fetch != None):
                hosthash = m_fetch.group(1)
                plugin = m_fetch.group(2)
                print "fetch " + hosthash + " " + plugin
                hostname = self._get_host_from_hash(hosthash)
                if (hostname != ''):
                    if ( not (self._clients.has_key(hostname)) ):
                        self._open_host(hostname)
                    reactor.callLater (0.1, self._send_line, hostname, 'fetch', plugin)
#                    self.sendLine("# fetch plugin " + plugin + " for " + hostname)

            if (m_config != None):
                hosthash = m_config.group(1)
                plugin = m_config.group(2)
                print "config " + hosthash + " " + plugin
                hostname = self._get_host_from_hash(hosthash)
                if (hostname != ''):
                    if ( not (self._clients.has_key(hostname)) ):
                        self._open_host(hostname)
                    reactor.callLater (0.1, self._send_line, hostname, 'config', plugin)
#                    self.sendLine("# config plugin " + plugin + " for " + hostname)

    def dataReceived(self, data):
        print "received : ", data
        data = data.replace("\r\n", "\n")
        data = data.replace("\r", "\n")
        data = data.replace("\n\n", "\n")
        # [:-1] because the split gives an empty value after the final \n from command list
        for l in data.split("\n")[:-1]:
          if ( l == ''): # Empty line
            self.sendLine(self._help_line)
            break
          self._do_line(l)


class MuninRelayFactory(Factory):
    protocol = MuninRelay

    def __init__(self, config):
        self.cfg = config
            

def main():
    f = MuninRelayFactory(config)
    if ( not config.has_key('port') or (config['port'] == '') ):
        print "Missing port parameter in global config"
        sys.exit(-1)
    if ( not config.has_key('bind_address') or (config['bind_address'] == '') ):
        print "Missing bind_address parameter in global config"
        sys.exit(-1)
    reactor.listenTCP(int(config['port']), f, 50, config['bind_address'])
    if ( args.pid_file != '' ):
      try:
        ff = open(args.pid_file, "w")
        ff.write(str(os.getpid()))
        ff.close()
      except Exception, e:
        print "Error writing in %s :" % args.pid_file, e
    reactor.run()


parser = argparse.ArgumentParser(description='Munin relay.')
parser.add_argument('--debug', action='store_true', default=False, dest='debugme', help='do not detach process')
parser.add_argument('--pid', action='store', default='', dest='pid_file', help='Specify a PID file')

args=parser.parse_args()

# Background process
if (args.debugme == True):
  print "Debug mode: do not detach"
  main()
  sys.exit (0)
else:
  try:
      try:
          pid = os.fork()
      except OSError, e:
          syslog.syslog ("Failed to daemonize")
          raise Exception, "%s [%d]" % (e.strerror, e.errno)

      orig_stderr = sys.stderr
      if (pid == 0):  # the daemon child
        try:
           os.setsid()   # set own session
           os.chdir("/tmp")
           null = open('/dev/null', 'a')
           sys.stdout = null
           sys.stderr = null
           main()
        except Exception, e:
          print >>orig_stderr, "Error: failed to launch munin-relay :", e
          sys.exit (2)
      else:       # the parent
        syslog.syslog ("Launch munin-relay #"+str(pid))
        sys.exit (0)

  except KeyboardInterrupt:
      print "Interrupt"
      sys.exit (0)

