import pandas as pd
import nltk

from nltk.corpus import wordnet as wn
from nltk.corpus import sentiwordnet as swn
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
import string
from nltk.stem import WordNetLemmatizer

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split


'''
Loading test and train data for electronics reviews. 
Filter data by removing ratings which are neutral (3 stars) and choose ratings between length 1250 and 1500. 
'''

print('Loading data...')

train_elec = pd.read_csv('data/amz-electronics/raw/csv-train', names=['review', 'rating', 'product']).dropna()
test_elec = pd.read_csv('data/amz-electronics/raw/csv-test', names=['review', 'rating', 'product']).dropna()

test_elec['review length'] = test_elec['review'].apply(len)
test_elec = test_elec[(test_elec['review length'] > 1250) & (test_elec['review length'] < 1500)]
test_elec = test_elec[test_elec['rating'] != 3].reset_index()

train_elec['review length'] = train_elec['review'].apply(len)
train_elec = train_elec[(train_elec['review length'] > 1250) & (train_elec['review length'] < 1500)]
train_elec = train_elec[train_elec['rating'] != 3].reset_index()

print('Loading data finished... Now conducting sentiment analysis')

'''
Sentiment Analysis through the use of a lexicon.
I used Vader Sentiment Analyzer for determining sentiment for each review.
'''

def sentiment_analysis (data):
    sid = SentimentIntensityAnalyzer()
    s = data['review'].apply(lambda x: sid.polarity_scores(x.lower())['compound'])
    return s

train_elec['vader sentiment'] = sentiment_analysis(train_elec)
test_elec['vader sentiment'] = sentiment_analysis(test_elec)

train_elec['binary sentiment'] = train_elec['vader sentiment'] >= 0
test_elec['binary sentiment'] = test_elec['vader sentiment'] >= 0


# train data sentiment accuracy
print("Sentiment Accuracy for train data: ", accuracy_score(train_elec['rating'] > 3, train_elec['binary sentiment'])) 
# test data sentiment accuracy
print("Sentiment Accuracy for test data: ", accuracy_score(test_elec['rating'] > 3, test_elec['binary sentiment'])) 

print('Finished conducting sentiment analysis. Applying Normalization\n')

'''
Normalization. I apply lemmatizer to the text, which offers better stemming than PortStemmer. 
This also removes any puncutation marks. The stopwords are handleled later by the CountVectorizer. 
'''

lemmatizer = WordNetLemmatizer()

def normalization(text):
    text = text.lower().translate(str.maketrans('', '', string.punctuation))
    lemmatized = ' '.join([lemmatizer.lemmatize(word) for word in text.split(' ')])
    return lemmatized

train_elec['review clean'] = train_elec['review'].apply(normalization)
test_elec['review clean'] = test_elec['review'].apply(normalization)

print('Finished normalization. Starting vectorizing bigrams and unigrams')

'''
Vectorizing all possible unigrams and bigrams through CountVectorizer
'''

ngram_vectorizer = CountVectorizer(binary=True, ngram_range=(1, 2), stop_words=['in','of','at','a','the'])
ngram_vectorizer.fit(train_elec['review clean'])
X = ngram_vectorizer.transform(train_elec['review clean'])
X_test = ngram_vectorizer.transform(test_elec['review clean'])

'''
Logistic Regression for fitting the data
'''

lr = LogisticRegression(C=0.5)
lr.fit(X, train_elec['rating'] > 3)
print("Final Accuracy of Logistic Regression: %s\n" % accuracy_score(test_elec['rating'] > 3, lr.predict(X_test)))

'''
SVM for fitting the data
'''

svm = LinearSVC(C=0.5)
svm.fit(X, train_elec['rating'] > 3)
print("Final Accuracy of SVM: %s\n" % accuracy_score(test_elec['rating'] > 3, svm.predict(X_test)))

print("Manually picking bigrams...")

'''
Custom bigrams detection as mentioned in the Thumbs Up and Down paper. 
'''

def bigrams (f, s, t):
    if f[:2] == 'NN' and s[:2] == 'JJ' and t[:2] != 'NN':
        return True
    elif f[:2] == 'JJ':
        if s[:2] == 'JJ' and t[:2] != 'NN':
            return True
        elif s[:2] == 'NN':
            return True
    elif f[:2] == 'RB':
        if s[:2] == 'VB':
            return True
        elif s[:2] == 'JJ' and t[:2] != 'NN':
            return True
    return False
            

def features (data):
    phrases = []
    for index, row in data.iterrows():
        text = nltk.pos_tag(word_tokenize(row['review clean'].lower()))
        for i in range(3, len(text)):
            if bigrams(text[i-2][1], text[i-1][1], text[i][1]):
                if text[i-2][0] == "n't":
                    phrases.append(text[i-3][0] + text[i-2][0] + ' ' + text[i-1][0])
                else:
                    phrases.append(text[i-2][0] + ' ' + text[i-1][0])
    return phrases
        
bigrams = features(train_elec)

print('Finished finding bigrams. Vectorizing and fitting a SVM model...')

ngram_vectorizer = CountVectorizer(binary=True, ngram_range=(1, 2), stop_words=['in','of','at','a','the'])
ngram_vectorizer.fit(bigrams)
X = ngram_vectorizer.transform(train_elec['review clean'])
X_test = ngram_vectorizer.transform(test_elec['review clean'])

svm = LinearSVC(C=0.25)
svm.fit(X, train_elec['rating'] > 3)
print("Final Accuracy of custom bigrams: %s" % accuracy_score(test_elec['rating'] > 3, svm.predict(X_test)))