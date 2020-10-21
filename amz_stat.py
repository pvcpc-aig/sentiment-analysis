"""
Compile statistics for each amazon dataset.
"""
import os
import sys
import json
from pathlib import Path

import udax as dx


# A 2^21 = 2MB buffer size for reading.
read_buffer_size = 2 ** 21

# A 2^19 = 512Kb buffer size for writing.
write_buffer_size = 2 ** 19

# The dataset will be processed in 64K block
# chunks. This is mainly used to control feed
block_size = 2 ** 16


def gen_analytics(amz_ds):
	raw = amz_ds.joinpath("raw")
	json_f = raw.joinpath("json")
	stat_f = raw.joinpath("stat")
	print(f"Generating analytics of {amz_ds} into {stat_f}...")

	# Setup feedback utilities
	print(f"Gathering size information...")
	max_lines = dx.f_line_count(json_f, buffering=read_buffer_size)
	stopwatch = dx.Stopwatch()
	reporter = dx.BlockProcessReporter(block_size, max_lines)

	# Data to record
	tot_reviews = 0
	tot_items = 0
	tot_rating = 0

	item_stat = {}
	item_avg = 0

	rating_stat = [ 0, 0, 0, 0, 0 ]
	rating_avg = 0

	# load and compute the data analytics
	print(f"Processing statistics for {amz_ds}...")
	stopwatch.start()
	with open(json_f, mode="r", buffering=read_buffer_size) as json_h:
		for line in json_h:
			obj = json.loads(line)

			item_id = obj["asin"]
			item_rating = obj["overall"]
			n_item_rating = int(float(item_rating))

			tot_reviews += 1
			tot_rating += item_rating
			
			if item_id not in item_stat:
				item_stat[item_id] = 1
				tot_items += 1
			else:
				item_stat[item_id] += 1

			if 1 <= n_item_rating and n_item_rating <= 5:
				rating_stat[n_item_rating - 1] += 1
			
			reporter.ping()

		item_avg = tot_reviews / tot_items
		rating_avg = tot_rating / tot_reviews

		reporter.finish()
	stopwatch.stop()
	print(f"Done processing in {repr(stopwatch)}")

	# Save the statistics to a file
	print(f"Saving statistics to {stat_f}...")
	with open(stat_f, mode="w", buffering=write_buffer_size) as stat_h:
		stat_h.write(f"Total reviews: {tot_reviews}\n")
		stat_h.write(f"Total items: {tot_items}\n")
		stat_h.write(f"Average reviews/item: {item_avg}\n")
		stat_h.write("\n")
		stat_h.write(f"Total 5/5 ratings: {rating_stat[4]}, {rating_stat[4] * 100 / tot_reviews} %\n")
		stat_h.write(f"Total 4/5 ratings: {rating_stat[3]}, {rating_stat[3] * 100 / tot_reviews} %\n")
		stat_h.write(f"Total 3/5 ratings: {rating_stat[2]}, {rating_stat[3] * 100 / tot_reviews} %\n")
		stat_h.write(f"Total 2/5 ratings: {rating_stat[1]}, {rating_stat[3] * 100 / tot_reviews} %\n")
		stat_h.write(f"Total 1/5 ratings: {rating_stat[0]}, {rating_stat[3] * 100 / tot_reviews} %\n")
		stat_h.write(f"Average Rating: {rating_avg}\n")
		stat_h.write("\n")
		stat_h.write("%12s %8s %8s\n" % ("Item ID", "Freq", "% Freq"))
		for item, count in sorted(item_stat.items(), key=lambda x: x[1], reverse=True):
			stat_h.write("%12s %8d %8.3f %%\n" % (item, count, 100 * count / tot_items))
	print("Ok.")
			

if __name__ == "__main__":
	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			gen_analytics(dataset)
