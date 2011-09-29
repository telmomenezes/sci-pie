#!/usr/bin/env python
# encoding: utf-8

__author__ = "Telmo Menezes (telmo@telmomenezes.com)"
__date__ = "Jun 2011"


import sys
import sqlite3
from syn.net import Net


def citations2syn(dbpath, outpath):

    net = Net(outpath)

    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    
    nodes = {}
    timestamps = {}
    cur.execute("SELECT id, title, timestamp FROM articles")
    for row in cur:
        label = '%s [%d]' % (row[1], row[0])
        nodes[row[0]] = net.add_node(label=label)
	timestamps[row[0]] = row[2]

    cur.execute("SELECT orig_id, targ_id FROM citations WHERE targ_id>=0")
    for row in cur:
	try:
             net.add_edge(nodes[row[0]], nodes[row[1]], timestamps[row[0]])
	except:
	     print 'oops.'

    cur.close()
    conn.close()

    print('Done.')


if __name__ == '__main__':
    citations2syn(sys.argv[1], sys.argv[2])
