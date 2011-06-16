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


def authorcitations(dbpath):
    count = 0

    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    cur2 = conn.cursor()
    cur3 = conn.cursor()
    cur4 = conn.cursor()

    cur.execute("SELECT orig_id, targ_id FROM citations WHERE targ_id>=0")
    for row in cur:
        print 'processing citation #%d' % count
        count += 1
        article_orig_id = row[0]
        article_targ_id = row[1]
        cur2.execute("SELECT timestamp FROM articles WHERE id=%d" % article_orig_id)
        row2 = cur2.fetchone()
        if row2 is None:
            continue
        citation_ts = float(row2[0])

        cur2.execute("SELECT author_id FROM article_author WHERE article_id=%d" % article_orig_id)
        for row2 in cur2:
            author_orig_id = row2[0]
            cur3.execute("SELECT author_id FROM article_author WHERE article_id=%d" % article_targ_id)
            for row3 in cur3:
                author_targ_id = row3[0]

                cur4.execute("SELECT id, timestamp FROM author_citations WHERE orig_id=%d AND targ_id=%d" % (author_orig_id, author_targ_id))
                row4 = cur4.fetchone()
                if row4 is None:
                    cur4.execute("INSERT INTO author_citations (orig_id, targ_id, timestamp) VALUES (%d, %d, %f)" % (author_orig_id, author_targ_id, citation_ts))
                else:
                    ca_id = row4[0]
                    ca_ts = float(row4[1])
                    if ca_ts > citation_ts:
                        cur4.execute("UPDATE author_citations SET timestamp=%f WHERE id=%d" % (citation_ts, ca_id))

    conn.commit()
    cur.close()
    cur2.close()
    cur3.close()
    cur4.close()
    conn.close()

    print('Done.')


if __name__ == '__main__':
    authorcitations(sys.argv[1])
