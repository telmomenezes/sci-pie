#!/usr/bin/env python


__author__ = "Telmo Menezes (telmo@telmomenezes.com)"
__date__ = "Feb 2011"


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
    safe_execute(cur, "ALTER TABLE citations ADD COLUMN target_id INTEGER")
        
    # indexes
    #self.safe_execute(cur, "CREATE INDEX link_blog_orig_targ ON link (blog_orig, blog_targ)")

    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    create_db(sys.argv[1])
