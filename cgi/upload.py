#!/usr/bin/python
import cgi
import cgitb
cgitb.enable()

import solr
import MySQLdb
import MySQLdb.cursors
import json

s = solr.SolrConnection('http://localhost:8983/solr')
print "Content-type:text/html\r\n"

class Uploader:
    """Indexes all the concept and columns received from POST and
       associate them with the table"""
    def __init__(self):
        self.topics = []
        self.db = MySQLdb.connect(user="root", passwd="password", \
            db="wikidb")
        self.cursor1 = self.db.cursor()
        self.db2 = MySQLdb.connect(user="root", passwd="password", \
            db="wikidb")
        self.cursor2 = self.db2.cursor()

    def index(self, table_id, columns):
        """ take a list of words, and return the weighing of the 
            closest meaning"""
        # we first process the words to make sure that all possible entries
        # will be in the database
        print 'printing table id', table_id

        for word in columns:
            # because phrases in columns did not exist before, 
            # we need to add that to the lucene index
            # will insert into page to get an id
            # page_namespace will be 16 
            # currently the id goes up to 38713350
            # all non-entity starts from 100000000
            word = word.replace("(", "\(") 
            word = word.replace(")", "\)") 
            word = word.replace(":", "\:") 
            # first check it is not in solr yet
            result, result_data = self.find_exact_matches(word)
            if result != [] and len(result) == 1:
                self.insert_edge(table_id, result_data[0]['id'])
                s.add(id=result_data[0]['id'], name=word.replace("_", " "))
                s.commit()

            elif result != [] and len(result) > 1: 
                # there are exact match. If there are more than one
                # exact match, then we go for the one with entity
                ids_with_namespace = self.retrieve_namespace(result)

                # if the item exists in entity space, then add the item
                ent_namespace = [x[0] for x in ids_with_namespace if int(x[1])== 0]
                cat_namespace = [x[0] for x in ids_with_namespace if int(x[1]) == 14]
                row_namespace = [x[0] for x in ids_with_namespace if int(x[1]) == 16]
                rest_namespace = [x[0] for x in ids_with_namespace if int(x[1]) \
                    not in (14, 16, 0)]

                if ent_namespace != []:
                    insert_id = ent_namespace[0]
                elif cat_namespace != []:
                    insert_id = cat_namespace[0]
                elif row_namespace != []:
                    insert_id = row_namespace[0]
                else:
                    insert_id = rest_namespace[0]
                old_doc = [x for x in result_data if int(x["id"]) == insert_id][0]
                s.add(id=insert_id, name=old_doc["name"].replace("_", " "))
                s.commit()
                self.insert_edge(table_id, insert_id)
                
                
            else: # no exact match, insert it and add it
                self.cursor1.execute("""
                    INSERT INTO row_name (name) VALUE (%s)""", (word.encode('utf-8'),))
                last_id = self.cursor1.lastrowid
                s.add(id=last_id, name=word.replace("_", " "))
                s.commit()
                self.insert_edge(table_id, last_id)
    
    def find_exact_matches(self, word):  
        """query in sorl and see if there are any exact matches
           return a non empty list if there is any exact match""" 
        word = word.replace('_', ' ')
        words_from_lucene = self.query_lucene(word)
        matched_ids = [x['id'] for x in words_from_lucene \
            if x['name'] == word]
        # filter out the general result as well
        words_from_lucene = [x for x in words_from_lucene if x["name"] == word]
        return matched_ids, words_from_lucene

    def query_lucene(self, word):
        """helper function to get all the matches for the word from lucene"""
        response = s.query('name:'+"(" + word
            .replace('%2B', '+').replace('/','\/') +")")
        return response.results 

    def retrieve_namespace(self, ids):
        """Given ids for the entities, retrieve the corresponding namespace"""

        row_ids = [x for x in ids if int(x) >= 100000000]
        ids = [x for x in ids if int(x) < 100000000]

        entities = []        
        if ids != []: 
            self.cursor2.execute("""
                SELECT page_id, page_namespace FROM page where page_id in
                (""" + ",".join(ids) + ")")
            result = self.cursor2.fetchall()
            entities += result

        if row_ids != []:
            self.cursor2.execute("""
                SELECT id, 16 FROM row_name where id in
                (""" + ",".join(row_ids) + ")")
            entities += self.cursor2.fetchall()
        return entities

    def insert_edge(self, table_id, entity_id):
        """Insert the edge between the table_id and the entity/cat/row"""
        
        self.cursor2.execute("""
            INSERT IGNORE INTO table_id_to_entity VALUES (%s, %s)
            """, (str(table_id), entity_id))

        self.cursor2.execute("""
            INSERT IGNORE INTO entity_to_table_id VALUES (%s, %s)
            """, (entity_id, str(table_id)))
        

def main():
    """ Init the Uploader, get the POST data for 'column' and table_id,
        and then index them by putting it into table_id_to_entity,
        entity_to_table_id and solr"""
    ana = Uploader()
    fields = cgi.FieldStorage()
    column = json.loads(fields.getvalue('column'))
    table_id = int(fields.getvalue('tableid'))
    ana.index(table_id, column)


if __name__ == "__main__":
    main()
