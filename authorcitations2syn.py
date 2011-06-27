#!/usr/bin/env python
# encoding: utf-8

__author__ = "Telmo Menezes (telmo@telmomenezes.com)"
__date__ = "Jun 2011"


"""
Copyright (C) 2011 Telmo Menezes.

This program is free software; you can redistribute it and/or modify
it under the terms of the version 2 of the GNU General Public License 
as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
"""


import sys
import sqlite3
from syn.net import Net


def authorcitations2syn(dbpath, outpath):

    net = Net(outpath)
    f = open(outpath, 'w')
    net.crete_db()

    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    
    nodes = {}
    cur.execute("SELECT id FROM authors")
    for row in cur:
        nodes[row[0]] = net.add_node()

    cur.execute("SELECT orig_id, targ_id, timestamp FROM author_citations")
    for row in cur:
        net.add_edge(nodes[row[0]], nodes[row[1]], row[2])

    cur.close()
    conn.close()

    print('Done.')


if __name__ == '__main__':
    authorcitations2syn(sys.argv[1], sys.argv[2])
