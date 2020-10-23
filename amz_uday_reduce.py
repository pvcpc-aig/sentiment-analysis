"""
This script reduces the amazon datasets based on
deduction from the statistics of the amazon electronics
review dataset.


amz_electronics
--------------------------------------------------------------------------------
Total reviews: 1689188
Total items: 63001
Average reviews/item: 26.812

Total 5/5 ratings: 1009026 (59.734 %)
Total 4/5 ratings: 347041 (20.545 %)
Total 3/5 ratings: 142257 (8.422 %)
Total 2/5 ratings: 82139 (4.863 %)
Total 1/5 ratings: 108725 (6.437 %)
Average Rating: 4.223

     Item ID       Freq     Freq %    5/5 %    4/5 %    3/5 %    2/5 %    1/5 %
  B007WTAJTO       4914    7.800 %   79.8 %   10.7 %    2.9 %    1.6 %    5.0 %
  B003ES5ZUU       4142    6.574 %   85.4 %   11.2 %    2.1 %    0.6 %    0.7 %
  B00DR0PDNE       3797    6.027 %   50.1 %   21.6 %   13.6 %    7.4 %    7.3 %
  B0019EHU8G       3434    5.451 %   87.8 %    8.2 %    1.8 %    0.7 %    1.5 %
  B002WE6D44       2812    4.463 %   81.4 %   11.5 %    2.3 %    1.4 %    3.4 %
  B003ELYQGG       2651    4.208 %   63.8 %   19.5 %    8.9 %    4.1 %    3.7 %
  B0002L5R78       2598    4.124 %   79.0 %   11.9 %    3.2 %    1.9 %    4.0 %
  B009SYZ8OC       2541    4.033 %   71.5 %   13.8 %    6.6 %    3.9 %    4.2 %
  B00BGGDVOO       2103    3.338 %   70.0 %   15.4 %    6.1 %    3.8 %    4.7 %
  B002V88HFE       2081    3.303 %   83.3 %   11.3 %    2.4 %    1.7 %    1.2 %
  ...
"""
import os
import sys
import json
import udax as dx
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pathlib import Path


sid = SentimentIntensityAnalyzer()

# Stopwords will be removed immediately to reduce
# storage requirements and processing time later.
useless_words = set(stopwords.words("english"))

# The desired size of the training and testing data
# reduction combined in review samples.
dataset_desired_size = 35000

# The ratio of the training samples to testing samples
# to extract.
train_to_test_ratio = 0.8

# The maximum number of reviews to extract from the
# whole dataset into a training set.
train_review_count = int(dataset_desired_size * train_to_test_ratio)

# The maximum number of reviews to extract from
# the whole dataset into a testing set.
test_review_count = int(dataset_desired_size * (1 - train_to_test_ratio)) 

# The minimum length, in characters, of the review.
min_len = 1250

# The maximum length, in characters, of the review.
max_len = 1500


def reduce_dataset(amz_ds):
	print(f"Reducing {amz_ds}...")
	raw = amz_ds.joinpath("raw")
	json_f = raw.joinpath("json")
	csv_f = raw.joinpath("csv-train")
	csv_test_f = raw.joinpath("csv-test")

	est_ratings_correct = 0
	reviews_tot = 0
	train_tot = 0
	test_tot = 0

	stopwatch = dx.Stopwatch()

	print("Processing...")
	stopwatch.start()
	with dx.f_open_large_write(csv_test_f) as csv_test_h:
		with dx.f_open_large_write(csv_f) as csv_h:
			with dx.f_open_large_read(json_f) as json_h:
				for ln in json_h:
					obj = json.loads(ln)
					text = obj["reviewText"]
					if len(text) < 1250 or len(text) > 1500:
						continue

					rating = int(float(obj["overall"]))
					if rating == 3:
						continue

					norm = dx.s_norm(text)
					words = norm.split()
					rnorm = ' '.join(filter(lambda x: x not in useless_words, words))

					score = sid.polarity_scores(rnorm)
					est_sentiment = score["compound"]
					est_rating = int((est_sentiment + 1) * 5 / 2) + 1 # convert from (-1, 1) to [1, 5]

					est_rating_correct = (rating > 3 and est_rating > 3) or \
										 (rating == 3 and est_rating == 3) or \
										 (rating < 3 and est_rating < 3)

					if est_rating_correct:
						est_ratings_correct += 1

					if train_tot < train_review_count:
						dx.csv_writeln(rnorm, rating, est_rating, est_rating_correct, stream=csv_h)
						train_tot += 1
					else:
						dx.csv_writeln(rnorm, rating, est_rating, est_rating_correct, stream=csv_test_h)
						test_tot += 1
					
					reviews_tot += 1

					if train_tot == train_review_count and test_tot == test_review_count:
						break
	
	stopwatch.stop()

	print(f"Done in {repr(stopwatch)}.")
	print("Vader achieved %.2f%% accuracy on the reduced dataset." % (100 * est_ratings_correct / reviews_tot))


if __name__ == "__main__":
	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			reduce_dataset(dataset)
