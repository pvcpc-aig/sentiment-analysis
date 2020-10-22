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
from pathlib import Path


# Stopwords will be removed immediately to reduce
# storage requirements and processing time later.
useless_words = set(stopwords.words("english"))

# The minimum number of words required for a review
# to be considered in the reduction process.
min_words_per_review = 8

# The maximum number of reviews to consider for any
# given `overall` rating.
max_per_rating = 2048


def reduce_dataset(amz_ds):
	print(f"Reducing {amz_ds}...")
	raw = amz_ds.joinpath("raw")
	json_f = raw.joinpath("json")
	csv_f = raw.joinpath("csv")

	stopwatch = dx.Stopwatch()
	
	max_reviews_acceptable = max_per_rating * 5
	rating_table = [ 0, 0, 0, 0, 0 ]
	reviews_tot = 0

	print("Processing...")
	stopwatch.start()
	with dx.f_open_large_write(csv_f) as csv_h:
		with dx.f_open_large_read(json_f) as json_h:
			for ln in json_h:
				obj = json.loads(ln)
				norm = dx.s_norm(obj["reviewText"])
				words = norm.split()
				if len(words) < min_words_per_review:
					continue

				rnorm = ' '.join(filter(lambda x: x not in useless_words, words))

				rating = int(float(obj["overall"]))
				if rating_table[rating - 1] < max_per_rating:
					dx.csv_writeln(rnorm, rating, stream=csv_h)
					rating_table[rating - 1] += 1
					reviews_tot += 1
				
				if reviews_tot == max_reviews_acceptable:
					break
	
	stopwatch.stop()
	print(f"Done in {repr(stopwatch)}")


if __name__ == "__main__":
	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			reduce_dataset(dataset)
