from gensim import corpora, models, similarities
from sklearn.feature_extraction.text import TfidfVectorizer
from gensim.models import hdpmodel, ldamodel
from itertools import izip
import nltk

def print_lda(doc1, doc2):
    corpus, dic1 = get_corpus(doc1)
    lda = ldamodel.LdaModel(corpus, id2word=dic1, num_topics=2)
    #lda = hdpmodel.HdpModel(corpus,id2word=dictionary)

    # But I am unable to print out the topics, how should i do it?
    for i in range(2):
        print lda.print_topic(i)
    #print lda.print_topics(topics=20, topn=10)
    index = similarities.MatrixSimilarity(lda[corpus])
    print index
    corpus2, dic2 = get_corpus(doc2)
    lda2 = ldamodel.LdaModel(corpus2, id2word=dic2, num_topics=2)
    for i in range(2):
        print lda2.__class__.__name__


def get_corpus(documents): 
    # remove common words and tokenize
    stoplist = set(nltk.corpus.stopwords.words('english'))
    texts = [[word for word in document.lower().split() if word not in stoplist]
             for document in documents]

    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]
    return (corpus, dictionary)

def get_similarity(doc1, doc2):
    vect = TfidfVectorizer(min_df=1)
    tfidf = vect.fit_transform([doc1, doc2])
    print (tfidf * tfidf.T).A[0][1]
