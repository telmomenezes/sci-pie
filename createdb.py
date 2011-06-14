#!/usr/bin/env python


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


def safe_execute(cur, query):
    try:
        cur.execute(query)
        print('Executed query: %s' % query)
    except sqlite3.OperationalError:
        pass


def create_db(dbpath):
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()

    # create journals table
    safe_execute(cur, "CREATE TABLE publications (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE publications ADD COLUMN title TEXT")
    safe_execute(cur, "ALTER TABLE publications ADD COLUMN iso_title TEXT")
    safe_execute(cur, "ALTER TABLE publications ADD COLUMN type TEXT")
    safe_execute(cur, "ALTER TABLE publications ADD COLUMN ISSN TEXT")

    # create issues table
    safe_execute(cur, "CREATE TABLE issues (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE issues ADD COLUMN wos_id TEXT")
    safe_execute(cur, "ALTER TABLE issues ADD COLUMN pub_id INTEGER")
    safe_execute(cur, "ALTER TABLE issues ADD COLUMN year INTEGER")
    safe_execute(cur, "ALTER TABLE issues ADD COLUMN date TEXT")
    safe_execute(cur, "ALTER TABLE issues ADD COLUMN volume INTEGER")
    safe_execute(cur, "ALTER TABLE issues ADD COLUMN issue INTEGER")
    safe_execute(cur, "ALTER TABLE issues ADD COLUMN timestamp REAL")

    # create articles table
    safe_execute(cur, "CREATE TABLE articles (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN wos_id TEXT")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN title TEXT")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN abstract TEXT")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN issue_id INTEGER")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN type TEXT")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN beginning_page INTEGER")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN end_page INTEGER")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN page_count INTEGER")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN language TEXT")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN timestamp REAL")
    
    # create authors table
    safe_execute(cur, "CREATE TABLE authors (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE authors ADD COLUMN name TEXT")
    
    # create article_author table
    safe_execute(cur, "CREATE TABLE article_author (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE article_author ADD COLUMN article_id INTEGER")
    safe_execute(cur, "ALTER TABLE article_author ADD COLUMN author_id INTEGER")
    
    # create keywords table
    safe_execute(cur, "CREATE TABLE keywords (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE keywords ADD COLUMN keyword TEXT")
    
    # create article_keyword table
    safe_execute(cur, "CREATE TABLE article_keyword (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE article_keyword ADD COLUMN article_id INTEGER")
    safe_execute(cur, "ALTER TABLE article_keyword ADD COLUMN keyword_id INTEGER")
    
    # create citations table
    safe_execute(cur, "CREATE TABLE citations (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE citations ADD COLUMN orig_id INTEGER")
    safe_execute(cur, "ALTER TABLE citations ADD COLUMN targ_id INTEGER")
    safe_execute(cur, "ALTER TABLE citations ADD COLUMN orig_wosid TEXT")
    safe_execute(cur, "ALTER TABLE citations ADD COLUMN targ_wosid TEXT")
    
    # create organizations table
    safe_execute(cur, "CREATE TABLE organizations (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE organizations ADD COLUMN name TEXT")
    safe_execute(cur, "ALTER TABLE organizations ADD COLUMN city TEXT")
    safe_execute(cur, "ALTER TABLE organizations ADD COLUMN province_state TEXT")
    safe_execute(cur, "ALTER TABLE organizations ADD COLUMN country TEXT")
    safe_execute(cur, "ALTER TABLE organizations ADD COLUMN postal_code TEXT")
    
    # create article_organization table
    safe_execute(cur, "CREATE TABLE article_organization (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE article_organization ADD COLUMN article_id INTEGER")
    safe_execute(cur, "ALTER TABLE article_organization ADD COLUMN organization_id INTEGER")

    #create author_citations table
    safe_execute(cur, "CREATE TABLE author_citations (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE author_citations ADD COLUMN orig_id INTEGER")
    safe_execute(cur, "ALTER TABLE author_citations ADD COLUMN targ_id INTEGER")
    safe_execute(cur, "ALTER TABLE author_citations ADD COLUMN timestamp REAL")
        
    # indexes
    safe_execute(cur, "CREATE INDEX articles_id ON articles (id)")
    safe_execute(cur, "CREATE INDEX articles_wos_id ON articles (wos_id)")
    safe_execute(cur, "CREATE INDEX issues_id ON issues (id)")
    safe_execute(cur, "CREATE INDEX issues_wos_id ON issues (wos_id)")
    safe_execute(cur, "CREATE INDEX publications_ISSN ON publications (ISSN)")
    safe_execute(cur, "CREATE INDEX authors_name ON authors (name)")
    safe_execute(cur, "CREATE INDEX keywords_keyword ON keywords (keyword)")
    safe_execute(cur, "CREATE INDEX organizations_name ON organizations (name)")
    safe_execute(cur, "CREATE INDEX author_citations_id ON author_citations (id)")

    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    create_db(sys.argv[1])
