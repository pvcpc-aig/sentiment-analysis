"""
Takes in data from Julian McAuley's "JSON" datasets
and converts them into CSV fragment files with
some fixed number of lines per CSV file.
"""
import os
import sys
import json
import pandas as pd
from pathlib import Path


# Value in the range [0, 1] specifying the portion,
# as a percentage, of each dataset to convert to
# CSV.
max_dataset_portion = 0.25

# 2^17 lines per file, which should yield approximately
# 64 files for the 9 GB "json".
max_csv_lines = 131072


def raw_to_json(amz_ds):
	raw = amz_ds.joinpath("raw")
	json_f = raw.joinpath("json")
	json_c = 0

	with json_f.open(mode="r", encoding="utf-8") as handle:
		for i, l in enumerate(handle):
			json_c += 1
	json_c = int(json_c * max_dataset_portion)

	with json_f.open(mode="r", encoding="utf-8") as handle:
		n_csv = 0
		n_line = 0
		df = pd.DataFrame([], [])

		for line in handle:
			df.append(json.loads(line), ignore_index=True)
			print(df)
			break
			n_line += 1
			print("[%5.1f%%] Processed of %5.1f%% portion" % (
				100 * n_line / json_c, 
				100 * max_dataset_portion
			))
			if n_line % max_csv_lines == 0:
				print(f"Writing CSV fragment {n_csv}")
				df.to_csv(raw.joinpath(f"csv-{n_csv}"))
				df.drop(df.index, inplace=True)
				n_csv += 1

			if n_line > json_c:
				break
		
		if len(df.index) > 0:
			df.to_csv(raw.joinpath(f"csv-{n_csv}"))


if __name__ == "__main__":
	if max_dataset_portion <= 0:
		print("max_dataset_portion must be > 0")
		sys.exit(0)

	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			print(f"Converting dataset {str(dataset)}")
			raw_to_json(dataset)