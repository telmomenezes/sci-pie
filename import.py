#!/usr/bin/env python
# encoding: utf-8
"""
untitled.py

Created by Telmo Menezes on 2011-03-07.
Copyright (c) 2011 Telmo Menezes. All rights reserved.
"""

import sys
import os
import sqlite3


class ImportWoS:

    def __init__(self):
        self.tag = ''
        self.data = ''
        
        self.clean_publication_issue()
        self.clean_article()
        self.clean_organization()

    def clean_publication_issue(self):
        self.publication = {}
        self.issue = {}
        self.disciplines = []
        
    def clean_article(self):
        self.article = {}
        self.authors = []
        self.keywords = []
        self.references = []
        
    def clean_organization(self):
        self.organization = {}

    def pub_id(self):
        cur = self.conn.cursor()

        cur.execute("SELECT id FROM publications WHERE iso_title=?", (self.publication['iso_title'],))
        row = cur.fetchone()
        if row is None:
            cur.execute("INSERT INTO publications (title, iso_title, type, ISSN) VALUES (?, ?, ?, ?)",
                (self.publication['title'],
                self.publication['iso_title'],
                self.publication['type'],
                self.publication['ISSN']))

            id = cur.lastrowid
            cur.close()
            return id
        else:
            cur.close()
            return row[0]

    def issue_id(self):
        cur = self.conn.cursor()
        
        cur.execute("SELECT id FROM issues WHERE wos_id=?", (self.issue['id'],))
        row = cur.fetchone()
        if row is None:
            date = ''
            if 'date' in self.issue:
                date = self.issue['date']
            issue = '-1'
            if 'issue' in self.issue:
                issue = self.issue['issue']
            cur.execute("INSERT INTO issues (wos_id, pub_id, year, date, volume, issue) VALUES (?, ?, ?, ?, ?, ?)",
                (self.issue['id'],
                self.pub_id(),
                self.issue['year'],
                date,
                self.issue['volume'],
                issue))
            
            id = cur.lastrowid
            cur.close()
            return id
        else:
            cur.close()
            return row[0]

    def author_id(self, author_name):
        cur = self.conn.cursor()
        
        cur.execute("SELECT id FROM authors WHERE name=?", (author_name,))
        row = cur.fetchone()
        if row is None:
            cur.execute("INSERT INTO authors (name) VALUES (?)", (author_name,))
            id = cur.lastrowid
            cur.close()
            return id
        else:
            cur.close()
            return row[0]

    def write_authors(self, article_id):
        cur = self.conn.cursor()
        
        for author_name in self.authors:
            aid = self.author_id(author_name)
            cur.execute("INSERT INTO article_author (article_id, author_id) VALUES (?, ?)",
                (article_id, aid))

        cur.close()

    def keyword_id(self, keyword):
        cur = self.conn.cursor()

        cur.execute("SELECT id FROM keywords WHERE keyword=?", (keyword,))
        row = cur.fetchone()
        if row is None:
            cur.execute("INSERT INTO keywords (keyword) VALUES (?)", (keyword,))
            id = cur.lastrowid
            cur.close()
            return id
        else:
            cur.close()
            return row[0]

    def write_keywords(self, article_id):
        cur = self.conn.cursor()

        for keyword in self.keywords:
            kid = self.keyword_id(keyword)
            cur.execute("INSERT INTO article_keyword (article_id, keyword_id) VALUES (?, ?)",
                (article_id, kid))

        cur.close()

    def write_article(self):
        
        cur = self.conn.cursor()
        
        abstract = ''
        if 'abstract' in self.article:
            abstract = self.article['abstract']

        cur.execute(("INSERT INTO articles (wos_id, title, abstract, issue_id, type, beginning_page, end_page, page_count, language)"
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"),
            (self.article['id'],
            self.article['title'],
            abstract,
            self.issue_id(),
            self.article['type'],
            self.article['beginning_page'],
            self.article['end_page'],
            self.article['page_count'],
            self.article['language']))
        
        article_id = cur.lastrowid
        
        self.write_authors(article_id)
        self.write_keywords(article_id)
        
        self.conn.commit()
        cur.close()
        
        print "Article:", self.article['title'] 

    def process(self, newtag, newdata):
        if newtag == '--':
            self.data = '%s%s' % (self.data, newdata)
            return
            
        nexttag = newtag
        nextdata = newdata
        
        tag = self.tag
        data = self.data
        
        # publication
        if tag == 'PT':
            self.publication['type'] = self.data
        elif tag == 'SO':
            self.publication['title'] = self.data
        elif tag == 'JI':
            self.publication['iso_title'] = self.data
        elif tag == 'SN':
            self.publication['ISSN'] = self.data
            
        # discipline
        elif tag == 'SC':
            self.disciplines.append(self.data)
    
        # issue
        elif tag == 'UI':
            self.issue['id'] = self.data
        elif tag == 'RE':
            self.clean_publication_issue()
        elif tag == 'VL':
            self.issue['volume'] = self.data
        elif tag == 'IS':
            self.issue['issue'] = self.data
        elif tag == 'PY':
            self.issue['year'] = self.data
        elif tag == 'PD':
            self.issue['date'] = self.data
        
        # article
        elif tag == 'UT':
            # START ARTICLE:
            pass
        elif tag == 'EX':
            self.write_article()
            self.clean_article()
        elif tag == 'T9':
            self.article['id'] = self.data
        elif tag == 'TI':
            self.article['title'] = self.data
        elif tag == 'AB':
            self.article['abstract'] = self.data
        elif tag == 'DT':
            self.article['type'] = self.data
        elif tag == 'BP':
            self.article['beginning_page'] = self.data
        elif tag == 'EP':
            self.article['end_page'] = self.data
        elif tag == 'PG':
            self.article['page_count'] = self.data
        elif tag == 'LA':
            self.article['language'] = self.data
        elif tag == 'AU':
            self.authors.append(self.data)
        elif tag == 'DE':
            self.keywords.append(self.data)
        elif tag == 'RP':
            # REPRINT ADDRESS
            pass
        elif tag == 'C1':
            # RESEARCH ADDRESS
            pass
        elif tag == 'EA':
            self.clean_organization()
        elif tag == 'NY':
            self.organization['city'] = self.data
        elif tag == 'NP':
            self.organization['province_state'] = self.data
        elif tag == 'NU':
            self.organization['country'] = self.data
        elif tag == 'NZ':
            self.organization['postal_code'] = self.data
        elif tag == 'NC':
            self.organization['name'] = self.data
        
        # cited reference
        elif tag == 'R9':
            self.references.append(self.data)
        
        self.tag = nexttag
        self.data = nextdata
    

    def main(self):
        self.conn = sqlite3.connect('mdts11.db')
        
        fd = open("/Users/telmo/Desktop/mdts11_raw/IC3N000252")
        line = fd.readline()
        while (line != "" ):
            tag = line[0:2]
            data = line[2:].strip()
            self.process(tag, data)
            line = fd.readline()
        self.process('', '')

        self.conn.close()


if __name__ == '__main__':
    ImportWoS().main()

