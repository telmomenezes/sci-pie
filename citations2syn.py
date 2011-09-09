#!/usr/bin/env python
# encoding: utf-8

__author__ = "Telmo Menezes (telmo@telmomenezes.com)"
__date__ = "Jun 2011"


import sys
import sqlite3


def citations2net(dbpath, outpath):

    f = open(outpath, 'w')

    f.write('[nodes]\n')

    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()

    idts = {}

    cur.execute("SELECT id, timestamp FROM articles")
    for row in cur:
        f.write('id=%d\n' % row[0])
        idts[row[0]] = row[1]

    f.write('[edges]\n')
    
    cur.execute("SELECT orig_id, targ_id FROM citations")
    for row in cur:
        if row[0] in idts:
            f.write('orig=%d targ=%d ts=%d\n' % (row[0], row[1], idts[row[0]]))

    cur.close()
    conn.close()
    f.close()

    print('Done.')


if __name__ == '__main__':
    citations2net(sys.argv[1], sys.argv[2])
