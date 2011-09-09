#!/usr/bin/env python
# encoding: utf-8

__author__ = "Telmo Menezes (telmo@telmomenezes.com)"
__date__ = "Jun 2011"


import sys
import sqlite3
from syn.net import Net


def authorcitations2syn(dbpath, outpath):

    net = Net(outpath)

    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    
    nodes = {}
    cur.execute("SELECT id, name FROM authors")
    for row in cur:
        label = '%s [%d]' % (row[1], row[0])
        nodes[row[0]] = net.add_node(label=label)

    cur.execute("SELECT orig_id, targ_id, timestamp FROM author_citations")
    for row in cur:
        net.add_edge(nodes[row[0]], nodes[row[1]], row[2])

    cur.close()
    conn.close()

    print('Done.')


if __name__ == '__main__':
    authorcitations2syn(sys.argv[1], sys.argv[2])
