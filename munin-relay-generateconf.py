#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4

# loosely based on Twisted Matrix Laboratories examples

import re
import time
import sys
import md5
import ConfigParser
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
  'bind_address': '10.23.23.90',
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
    tmp = {}
    for op in ccfg.options(section_name):
      tmp[op]=ccfg.get(section_name,op)
    config['hosts'].append( tmp )

if ( not config.has_key('allowed_ip') or len(config['allowed_ip']) == 0 ):
  print "Problem with configuration files, no one is allowed"
  sys.exit()

print "## Add these to your munin configuration"
print "## on the server running munin-server"
for h in config['hosts']:
  print "["+h['hostname']+"]"
  print "  address " + config['bind_address']
  print "  use_node_name no"
  print "  port " + config['port']
  print ""
