#!/usr/bin/python
import cgi
import cgitb
cgitb.enable()


import solr
import MySQLdb
import MySQLdb.cursors
import operator
import json

s = solr.SolrConnection('http://localhost:8983/solr')

class Analyzer:
    def __init__(self):
        self.topics = []
        self.db=MySQLdb.connect(user="root", passwd="password", \
            db="wikidb")
        self.cursor1 = self.db.cursor()

    # take a list of words, and return the weighing of the closest meaning
    def match(self, words):
        # for each word, query the lucene database for the top match
        # find the parent of the words
        # count the number of intersects of the words
        parents = []
        for word in words:
            top_match = self.query_lucene(word)
            temp = self.find_parents(top_match)
            temp = set([x[2] for x in temp])
            parents += temp
        names = parents
        inter = {} 
        for name in names:
            if name not in inter:
                inter[name] =1 
            else:
                inter[name] += 1
        inter = dict([(k,v) for k,v in inter.iteritems() if v != 1])
        inter = sorted(inter.iteritems(), key=operator.itemgetter(1))
        return inter
        
    def query_lucene(self, word):
        print 'name:'+"("+" ".join(word.split(" "))+")"
        response = s.query('name:'+"("+" ".join(word.split(" "))+")")
        ids = []
        top_hit = None
        for hit in response.results:
            #if top_hit:
            #    if float(hit['score']) < (top_hit * .5):
            #        break
            #else:
            #    top_hit = hit['score']
            ids.append(hit['id'])
        return ids 

    def find_parents(self, top_match):
        if len(top_match) == 0:
            return []
        query = """
            SELECT parent, `tf-idf`, page_title from final_cats_invert
            JOIN page on parent = page_id
            WHERE child IN (""" +",".join(top_match) +""")
            ORDER BY `tf-idf`
            """
        self.cursor1.execute(query)
        result = self.cursor1.fetchall()
        return result
        

def main():
    ana = Analyzer()
    #ana.match(['Austria', 'France', 'Germany'])
    #ana.match(['Google Sites', 'Yahoo! Sites','AOL, Inc.',
    #    'Facebook'])
    names = cgi.FieldStorage().getvalue('names').split(',')
    result = ana.match(names)
    #result = ana.match(['Apple Inc', 'Samsung', 'HTC', 'Motorola', 'LG'])
    result = json.dumps(result)
    print "Content-type:text/html\r\n"
    print result;


if __name__ == "__main__":
    main()
