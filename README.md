# Sentiment Analysis

This repository contains code written to train various models, primarily
Naive Bayes and SVM, on review samples from the Amazon review dataset 
provided by Julian McAuley [here](http://jmcauley.ucsd.edu/data/amazon/).

Be aware, that we used the older dataset that spanned from 1996 to
2014, and not the new 2018 dataset.

# Training & Evaluating Models

The scripts require Python 3.8, `stanza`, `nltk`, `udax`, and `pandas` 
to run. I will add a `requirements.txt` once I have only the required
dependencies without other unnecessary libraries.

`udax` is not a package in any distribution at the moment, so you must
obtain the source from the [udax repository](https://github.com/pvcpc-aig/udax).
The library does not contain any native code, so this should be a rather
simple integration.

You must run Python in a directory with the following data structure:
```
<cwd>/
└─ data/
   └─ amz-<class-1>/
      └─ raw/
         └─ json
   └─ amz-<class-2>/
   ...
```
where 
- `<cwd>` is your current working directory which can be querried in
a unix-environment using `$ pwd`
- `<class-N>` is any particular category of items aggregated in the reviews
as listed on the (dataset repository)[http://jmcauley.ucsd.edu/data/amazon/].
For example, "books", "electronics", "Movies and TV", etc.

Our scripts  were trained and tested on `amz-electronics`, which is what is 
required by `nltk_sentiment_analysis.py` and what is recommended for a balance
of size and speed.

The logistic regression and SVM implementations are all bundled in a single
script that filters and loads the dataset, `nltk_sentiment_analysis.py`.

The Naive Bayes implementation is dependent on a series of scripts executed in
the following order:
- Either `amz_reduce.py` (Max's reduction filter) or `amz_uday_reduce.py` (Uday's
  reduction filter) - to reduce the size of the original dataset from the order of
  millions to only a few thousand reviews with desired qualities. `csv-train` and
  `csv-test` are generated alongside `json` that split the training and testing
  data.
- `amz_gen_feature_index_table.py` - to create unigram, bigram, and Turney bigram tables
  from the given reduced dataset's `csv-train`.
- `amz_nb.py` - to evaluate Naive Bayes on the testing data, `csv-test`, and print a report
  to `nb.report`.

# Results (amz-electronics)

Naive Bayes (Max's reduction filter, without cross validation):
```
unigram accuracy: 55.703
all bigram accuracy: 62.891
turney bigram accuracy: 60.000
(results in percents)
```

Naive Bayes (Uday's reduction filter, without lemmatization, without cross validation):
```
unigram accuracy: 74.168
all bigram accuracy: 84.483
turney bigram accuracy: 84.841
(results in percents)
```

SVM (Uday's reduction filter, without cross validation):
```
[all bigram] Final Accuracy: 93.13201216315403 (reg. param. = 0.5)
[turney bigram] Final Accuracy: 92.64968019293279 (reg. param. = 0.1)
(results in percents)
```

# Replication

## Step 1, Filtering & Reducing:

Two types of reduction algorithms were used for Naive Bayes. Max's reduction filter and Uday's
reduction filter; the former being targeted toward rating uniformity and processing speed,
while the latter is targeted at review length uniformity and text simplification via lemmatization.

Uday's reduction filter showed significantly better results for Naive Bayes which likely stems from 
the fact that less human-set parameters are required, a slightly larger dataset is used, and better
heuristics were employed.

### Max's reduction filter:
- Pick several thousand reviews for training data, and several hundred for
testing data, that form a uniform rating distribution.
- Ensure any considered review has at least 8 words in the review text,
disgarding all punctuation and stopwords.
- Write these reviews to CSV files or convenient format.

### Uday's reduction filter:
- Pick approximately 35,000 reviews, 80% of which will be training data, and
20% will be testing data.
- Ensure any considered review has text with length in the range [1250, 1500]
characters, and an overall rating other than 3/5.
- Remove all punctuation and stopwords from the text, and lemmatize the 
review text.
- Write these reviews to CSV files or convenient format.

## Step 2, Count Vectorization & Table Generation:

Naive Bayes may be applied to unigrams, bigrams, and Turney bigrams as specified
in his 2002 paper [Thumbs Up or Thumbs Down](https://arxiv.org/ftp/cs/papers/0212/0212032.pdf).

The implementation for SVM uses bigrams and Turney bigrams.

### Unigrams
- Extract each word from the reduced dataset.

### All Bigrams
- Extract each consecutive bigram in the reduced dataset.
- Ignore the last word if it does not constitute a bigram.

### Turney Bigrams
- Extract each consecutive bigram matching the following requirements:

| First Word | Second Word | Third Word (not included) |
|------------|-------------|---------------------------|
| ADJ        | NOUN        | any                       |
| ADV        | ADJ         | not NOUN                  |
| ADJ        | ADJ         | not NOUN                  |
| NOUN       | ADJ         | not NOUN                  |
| ADV        | VERB        | any                       |

- Ignore the last word if it does not constitute a bigram.

For Naive Bayes, for each feature type, generate a table containing
all encountered features, their count in the positive context, their
count in the negative context, the total count of all features
in both the positive and negative contexts, and write them to a 
convenient location.

A positive review is indicated by an author rating of `> 3` and a negative
review is indicated by an author rating of `<= 3`. Note that, in the case
of Uday's reduction filter, there will be no reviews with a rating of 3/5,
therefore, a positive review and a negative review have the ratings `> 3`
and `< 3` respectively.

### Step 3, Evaluation:

Iterate over the review texts of the testing dataset created in step 1. 
Evaluate the model based on the accuracy of correct predictions.
