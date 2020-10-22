import os
import sys
import udax as dx
from pathlib import Path


# The dimension of the feature space.
feature_dim = 16


def gen_vector_tables(amz_ds):
	raw = amz_ds.joinpath("raw")
	csv_f = raw.joinpath("csv")
	tag_f = raw.joinpath("tag")

	all_uni_table_f     = raw.joinpath("all_uni.table")
	all_bi_table_f      = raw.joinpath("all_bi.table")
	turney_bi_table_f   = raw.joinpath("turney_bi.table")

	all_uni_vectors_f   = raw.joinpath(f"all_uni.{feature_dim}.vectors")
	all_bi_vectors_f    = raw.joinpath(f"all_bi.{feature_dim}.vectors")
	turney_bi_vectors_f = raw.joinpath(f"turney_bi.{feature_dim}.vectors")


	csv_h = dx.f_open_large_read(csv_f)
	tag_h = dx.f_open_large_read(tag_f)

	all_uni_vectors   = []
	all_bi_vectors    = []
	turney_bi_vectors = []


	for line in csv_h:
		data = dx.csv_parseln(line)



if __name__ == "__main__":
	# verify global settings
	if vector_size < 1:
		print("vector_size must be >= 1")
		sys.exit(1)

	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			gen_vector_tables(dataset)
