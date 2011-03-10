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
import os
import sqlite3
import datetime
import time


def trimyears(dbpath, begin_year, end_year):
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
        
    y0 = int(begin_year)
    y1 = int(end_year)
    ts0 = time.mktime(datetime.date(y0, 1, 1).timetuple())
    ts1 = time.mktime(datetime.date(y1, 12, 31).timetuple())
    
    print('Trimming database to time interval between the years of %d and %d.' % (y0, y1))
    
    cur.execute("SELECT count(id) FROM issues")
    row = cur.fetchone()
    issues = row[0]
    cur.execute("SELECT count(id) FROM articles")
    row = cur.fetchone()
    articles = row[0]
    cur.execute("SELECT count(id) FROM issues WHERE timestamp<? or timestamp>?", (ts0, ts1))
    row = cur.fetchone()
    discard_issues = row[0]
    cur.execute("SELECT count(id) FROM articles WHERE timestamp<? or timestamp>?", (ts0, ts1))
    row = cur.fetchone()
    discard_articles = row[0]

    issue_per = float(discard_issues) / float(issues) * 100.0
    article_per = float(discard_articles) / float(articles) * 100.0

    print('Of %d total issues, %d will be discarded. (%.2f%%)' % (issues, discard_issues, issue_per))
    print('Of %d total articles, %d will be discarded. (%.2f%%)' % (articles, discard_articles, article_per))

    cur.execute("DELETE FROM issues WHERE timestamp<? or timestamp>?", (ts0, ts1))
    cur.execute("DELETE FROM articles WHERE timestamp<? or timestamp>?", (ts0, ts1))

    conn.commit()
    cur.close()
    print("Done.")
        
    conn.close()


if __name__ == '__main__':
    trimyears(sys.argv[1], sys.argv[2], sys.argv[3])

