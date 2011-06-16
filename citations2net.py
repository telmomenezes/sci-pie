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


def citations2net(dbpath, outpath):

    f = open(outpath, 'w')

    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()

    cur.execute("SELECT orig_id, targ_id FROM citations")
    for row in cur:
        f.write('%d\t%d\n' % (row[0], row[1]))

    cur.close()
    conn.close()
    f.close()

    print('Done.')


if __name__ == '__main__':
    citations2net(sys.argv[1], sys.argv[2])
