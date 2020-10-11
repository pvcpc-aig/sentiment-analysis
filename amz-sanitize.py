"""
Takes in data from Julian McAuley's "JSON" datasets
and converts them into CSV fragment files with
some fixed number of lines per CSV file.
"""
import os
import sys
import json
import time
from pathlib import Path


# The datasets will be processed in `line_block_size`
# chunks, i.e. every `line_block_size` lines of a
# corpus file.
#
# Since we're not using multithreading, this determines
# how often a notification is printed to the user
# letting them know of the progress.
#
# The default value is 2^18 = ~256,000
line_block_size = 2 ** 18

# Value in the range (0, 1] specifying the portion,
# as a percentage, of each dataset to convert to
# CSV.
max_dataset_portion = 0.10

# The number of bytes to use as the read buffer for
# the input files.
#
# We choose 2^21 = 2MB as the buffer to minimize
# kernel context switches, and besides, most 64-bit
# operating systems should be able to take advantage
# of 2MB memory pages (windows may not lol)
max_read_buffer = 2 ** 21

# The number of bytes to use as the write buffer for
# output files.
#
# We choose 2^21 = 2MB, see reasoning for max_read_buffer
# above.
max_write_buffer = 2 ** 21


def raw_to_csv(ds_amz):
	from math import ceil

	# Setup information
	raw = ds_amz.joinpath("raw")
	json_f = raw.joinpath("json")
	csv_f = raw.joinpath("csv")

	tm_block_start = tm_block_end = None
	tm_whole_start = tm_whole_end = None

	# Gather dataset size information
	print(f"Gathering {ds_amz} size information...")
	ds_lines = 1
	with json_f.open(mode="r", buffering=max_read_buffer) as json_h:
		for i, _ in enumerate(json_h):
			ds_lines += 1
	ds_max_lines = int(ds_lines * max_dataset_portion)
	ds_max_blocks = (int(ceil(ds_max_lines / line_block_size)))

	# Load and convert JSON to CSV	
	ds_cur_block = 0 
	def _inc_feedback():
		nonlocal ds_max_blocks, ds_cur_block
		nonlocal tm_block_start, tm_block_end

		tm_block_end = time.monotonic_ns()

		ds_cur_block += 1
		percentage = 100 * ds_cur_block / ds_max_blocks
		elapsed = 1e-9 * (tm_block_end - tm_block_start)
		print(f"[%5.1f%%] Processed block {ds_cur_block}/{ds_max_blocks} in %.2fs" % (percentage, elapsed))

		tm_block_start = time.monotonic_ns()

	print(f"Processing {ds_amz} as %.1f%% dataset..." % (100 * max_dataset_portion))
	tm_whole_start = time.monotonic_ns()
	formatted_values = []
	with csv_f.open(mode="w", buffering=max_write_buffer) as csv_h:
		with json_f.open(mode="r", buffering=max_read_buffer) as json_h:
			i = 0
			tm_block_start = time.monotonic_ns() # a one-time startup thing for _inc_feedback()
			for i, raw_str in enumerate(json_h):
				if i != 0 and i % line_block_size == 0:
					_inc_feedback()

				parsed_values = json.loads(raw_str).values()
				while len(formatted_values) < len(parsed_values):
					formatted_values.append(None)

				for j, raw in enumerate(parsed_values):	
					formatted_values[j] = '"' + str(raw).replace('"', '\'') + '"'

				parsed_str = ','.join(formatted_values)
				csv_h.write(f"{parsed_str}\n")

				if i == ds_max_lines:
					break

			if i % line_block_size != 0: 
				_inc_feedback()

	tm_whole_end = time.monotonic_ns()			
	elapsed = 1e-9 * (tm_whole_end - tm_whole_start)

	print(f"Converted dataset {ds_amz} in %.2fs" % (elapsed))


if __name__ == "__main__":
	# verify global settings 
	if max_dataset_portion <= 0:
		print("max_dataset_portion must be > 0")
		sys.exit(0)

	# begin data processing
	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			print(f"Converting dataset {dataset}")
			raw_to_csv(dataset)