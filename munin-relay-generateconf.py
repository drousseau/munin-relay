#!/usr/bin/python3
# vim: tabstop=4 expandtab shiftwidth=4

# loosely based on Twisted Matrix Laboratories examples

from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
import re
import time
import sys
import cryptography
import configparser
from twisted.internet.protocol import Factory
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from configparser import ConfigParser

# Default conf
config = {
  'hosts': [],
  'allowed_ip': ['127.0.0.1'],
  'port': '4950',
  'bind_address': '10.23.23.90',
 }

# Read the configuration file
ccfg = ConfigParser()
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
    tmp = { 'port': '4949'}
    for op in ccfg.options(section_name):
      tmp[op]=ccfg.get(section_name,op)
    config['hosts'].append( tmp )

if ( 'allowed_ip' not in config or len(config['allowed_ip']) == 0 ):
  print("Problem with configuration files, no one is allowed")
  sys.exit()

print("## Add these to your munin configuration")
print("## on the server running munin-server")
for h in config['hosts']:
  print("["+h['hostname']+"]")
  print("  address " + config['bind_address'])
  print("  use_node_name no")
  print("  port " + config['port'])
  print("")
