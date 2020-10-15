"""
Generates a dictionary, i.e. list, of unique words found in the
review text of the amazon datasets.

This script should be run after `amz_sanitize.py` as it expects
the CSV files in the format

    '<review-text>,<overall>'

In every `amz-` prefixed dataset, this script will search for
all CSV files that are prefixed with `csv-`. For example, if
the dataset raw directory is structured as

    csv-train
    csv-test
    json

the script will read `csv-train` and `csv-test`. If the dataset
raw directory is

    csv
    json

it will just read `csv`.
"""
import io
import os
import sys
from pathlib import Path

from udax.strutil import surjective_punct_remove

# If the CSV files were exported with a header indicating the column names,
# this option can be set to `True` to skip that header line, i.e. the first
# line in the CSV.
skip_first_line = True


# The number of lines required to process in each CSV file before
# printing a status report. This is used to slow down the stdout
# stream to improve performance.
#
# 2^16 = 64K lines between each report.
lines_per_status_report = 2 ** 16


# For my system, because I often switch between a harddrive and 
# a solid state, I need to have the most amount of cache in memory
# for perfomrance, this is why the default value is 2^26 = 64MB
#
# This dictates the maximum buffer size, in bytes, when reading
# data from a file.
max_read_buffer = 2 ** 26


# Because the writing is minimal in this scenario, we only use
# a 1MB buffer instead of 64MB.
#
# This dictates the maximum buffer size, in bytes, when writing data
# to a file.
max_write_buffer = 2 ** 20


def parse_line(csv_line):
    row_cell_list = []
    
    buf = io.StringIO()
    quote = False
    escape = False

    for char in csv_line:
        if escape:
            buf.write(char)
            escape = False
            continue

        if char == '\\':
            escape = True
            continue

        if char == '\"':
            quote = not quote
            continue

        if char == ',' and not quote:
            row_cell_list.append(buf.getvalue())
            buf = io.StringIO()
            continue

        buf.write(char)

    left_over = buf.getvalue()
    if len(left_over) > 0:
        row_cell_list.append(left_over)

    return row_cell_list


def gen_word_freq_map(content):
    word_list = surjective_punct_remove(content.lower()).split()
    word_map = dict()

    for word in word_list:
        if word in word_map:
            word_map[word] += 1
        else:
            word_map[word] = 1

    return word_map


def merge_word_freq_map(into, foreign):
    for word, count in foreign.items():
        if word in into:
            into[word] += count
        else:
            into[word] = count


def gen_dict_for(amz_ds):
    from math import ceil

    raw = amz_ds.joinpath("raw")
    out_f = amz_ds.joinpath("dict")
    word_freq_map = dict()

    # Generate the word frequency dictionary
    for datafile in raw.iterdir():
        is_csv = datafile.name.startswith("csv")
        if is_csv:
            print(f"Processing {datafile}...")
            with datafile.open(mode="r", buffering=max_read_buffer) as csv_h:
                # Gather size information to report back to the user
                print("Gathering size information...")
                max_lines = 0
                if skip_first_line:
                    csv_h.readline()
                for line in csv_h:
                    max_lines += 1
                max_blocks = int(ceil(max_lines / lines_per_status_report))
                csv_h.seek(0)

                # Process the actual file
                print("Creating word table...")
                n_line = 0
                n_block = 0
                if skip_first_line:
                    csv_h.readline()
                for line in csv_h:
                    parsed_row = parse_line(line)
                    new_map = gen_word_freq_map(parsed_row[0])
                    merge_word_freq_map(word_freq_map, new_map)
                    n_line += 1
                    if n_line % lines_per_status_report == 0:
                        n_block += 1
                        print(f"[%5.1f%%] Processed block {n_block}/{max_blocks}" % (100 * n_block / max_blocks))

                if n_line % lines_per_status_report > 0:
                    print(f"[100.0%] Processed block {max_blocks}/{max_blocks}")
    
    # Write the dictionary out 
    print(f"Writing word table to {out_f}...")
    with out_f.open(mode="w", buffering=max_write_buffer) as out_h:
        # Export the dictionary as a file with ordered pairs, sorted by
        # frequency.
        #
        # The sorted function takes in the iterable of dictionary items,
        # sorts them by the count, and reverses the sort so that the greatest
        # number is the first.
        for pair in sorted(word_freq_map.items(), key=lambda x: x[1], reverse=True):
            out_h.write(f"{pair[0]} {pair[1]}\n")

    print("Done.")


if __name__ == "__main__":
    data = Path("data")
    for dataset in data.iterdir():
        is_amazon = dataset.name.startswith("amz")
        if is_amazon:
            print(f"Generating word table for {dataset}")
            gen_dict_for(dataset)
