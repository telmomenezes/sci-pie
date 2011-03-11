#!/usr/bin/env python
# encoding: utf-8

__author__ = "Telmo Menezes (telmo@telmomenezes.com)"
__date__ = "Mar 2011"


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


def articlenet2gexf(dbpath, outpath):

    f = open(outpath, 'w')

    f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    f.write('<gexf xmlns="http://www.gexf.net/1.1draft" version="1.1">\n')
    f.write('<graph mode="static" defaultedgetype="directed">\n')
    f.write('<nodes>\n')

    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()

    cur.execute("SELECT id FROM articles")
    for row in cur:
        articleid = row[0]
        #f.write('<node id="%s" label="%s" />\n' % (articleid, articleid))
        f.write('<node id="%s" />\n' % articleid)

    f.write('</nodes>\n')
    f.write('<edges>\n')

    edge_count = 0

    cur.execute("SELECT orig_id, targ_id FROM citations WHERE targ_id>=0")
    for row in cur:
        f.write('<edge id="%d" source="%d" target="%d" type="directed" />\n' % (edge_count, row[0], row[1]))
        edge_count += 1

    f.write('</edges>\n')
    f.write('</graph>\n')
    f.write('</gexf>\n')

    cur.close()
    conn.close()
    f.close()

    print('Done.')


if __name__ == '__main__':
    articlenet2gexf(sys.argv[1], sys.argv[2])
