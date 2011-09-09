#!/usr/bin/env python
# encoding: utf-8

__author__ = "Telmo Menezes (telmo@telmomenezes.com)"
__date__ = "Mar 2011"


import sys
import sqlite3


def articlenet2csv(dbpath, outpath):

    f = open(outpath, 'w')

    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()

    cur.execute("SELECT orig_id, targ_id FROM citations WHERE targ_id>=0")
    for row in cur:
        f.write('%d,%d\n' % (row[0], row[1]))

    cur.close()
    conn.close()
    f.close()

    print('Done.')


if __name__ == '__main__':
    articlenet2csv(sys.argv[1], sys.argv[2])
