import MySQLdb
import MySQLdb.cursors
import re
from creole import creole2html
from bs4 import BeautifulSoup
from gensim import corpora, models
import helper
import lda
import nltk


class Pages:
    def __init__(self):
        self.topics = []
        self.db=MySQLdb.connect(user="root", passwd="password", \
            db="wikidb", cursorclass=MySQLdb.cursors.SSCursor)
        self.cursor1 = self.db.cursor()

    def process(self):
        self.cursor1.execute("""select page_id, text.old_text from page 
			join text on text.old_id = page.page_latest 
			where page_id = 18938265 OR page_id = 3732122""")
        row1 = self.cursor1.fetchone()
        counter = 0
        parsed_content = creole2html(row1[1].decode('utf-8'))
        parsed_content = parsed_content.replace('&lt;', '<')
        parsed_content = parsed_content.replace('&gt;', '>')
        soup = BeautifulSoup(parsed_content)
        raw = nltk.clean_html(soup.__str__())
        row1 = self.cursor1.fetchone()
        parsed_content = creole2html(row1[1].decode('utf-8'))
        parsed_content = parsed_content.replace('&lt;', '<')
        parsed_content = parsed_content.replace('&gt;', '>')
        soup = BeautifulSoup(parsed_content)
        raw2 = nltk.clean_html(soup.__str__())
        #lda.print_lda([raw],[raw2])
        print lda.get_similarity(raw, raw2)
        counter += 1
        self.cursor1.close()
        self.db.close()

def main():
    topics = Pages()
    topics.process()

if __name__ == "__main__":
    main()
