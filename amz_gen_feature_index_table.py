import os
import sys
import stanza
import udax as dx
from pathlib import Path


pos_tagger = stanza.Pipeline(lang="en", processors="tokenize,pos")


def gen_feature_index_tables(amz_ds):
	print(f"Generating feature index tables for {amz_ds}...")
	raw = amz_ds.joinpath("raw")

	csv_f       = raw.joinpath("csv")
	tag_f       = raw.joinpath("tag")
	all_uni_f   = raw.joinpath("all_uni.table")
	all_bi_f    = raw.joinpath("all_bi.table")
	turney_bi_f = raw.joinpath("turney_bi.table")

	all_uni_table = {}
	all_bi_table = {}
	turney_bi_table = {}
	
	print("Gathering size information...")
	reporter = dx.BlockProcessReporter.file_lines(csv_f, block_size=16)
	stopwatch = dx.Stopwatch()

	print("Processing...")
	stopwatch.start()
	reporter.start()
	with dx.f_open_large_write(tag_f) as tag_h:
		with dx.f_open_large_read(csv_f) as csv_h:
			for line in csv_h:
				text, rating = dx.csv_parseln(line)

				words = text.split()
				pos = [word.upos for sent in pos_tagger(text).sentences for word in sent.words]
				
				# record unigrams
				for word in words:
					if word in all_uni_table:
						all_uni_table[word] += 1
					else:
						all_uni_table[word] = 1

				# record bigrams
				i = 0
				while i < len(words) - 1:
					w_first = words[i]
					w_second = words[i + 1]
					bigram = (w_first, w_second)

					t_first = pos[i]
					t_second = pos[i + 1]
					t_third = None if i >= len(words) - 2 else pos[i + 2]

					# all bigrams
					if bigram in all_bi_table:
						all_bi_table[bigram] += 1
					else:
						all_bi_table[bigram] = 1

					# turney bigrams:
					if bigram in turney_bi_table:
						turney_bi_table[bigram] += 1
					elif (t_first == "ADJ"  and t_second == "NOUN") or \
						 (t_first == "ADV"  and t_second == "VERB") or \
						 (t_first == "ADV"  and t_second == "ADJ" and t_third != "NOUN") or \
						 (t_first == "ADJ"  and t_second == "ADJ" and t_third != "NOUN") or \
						 (t_first == "NOUN" and t_second == "ADJ" and t_third != "NOUN"):
						turney_bi_table[bigram] = 1

					i += 2

				# save tags to an external CSV
				dx.csv_writeln(*pos, stream=tag_h)

				reporter.ping()
			reporter.finish()
	stopwatch.stop()
	print(f"Finished processing in {repr(stopwatch)}")

	print(f"Saving unigram table to {all_uni_f}...")
	with dx.f_open_large_write(all_uni_f) as all_uni_h:
		for uni, count in sorted(all_uni_table.items(), key=lambda x: x[1], reverse=True):
			all_uni_h.write(f"{uni} {count}\n")
	
	print(f"Saving all bigram table to {all_bi_f}...")
	with dx.f_open_large_write(all_bi_f) as all_bi_h:
		for bi, count in sorted(all_bi_table.items(), key=lambda x: x[1], reverse=True):
			all_bi_h.write(f"{bi[0]} {bi[1]} {count}\n")
	
	print(f"Saving turney bigram table to {turney_bi_f}...")
	with dx.f_open_large_write(turney_bi_f) as turney_bi_h:
		for bi, count in sorted(turney_bi_table.items(), key=lambda x: x[1], reverse=True):
			turney_bi_h.write(f"{bi[0]} {bi[1]} {count}\n")

	print("Ok")

				
if __name__ == "__main__":
	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			gen_feature_index_tables(dataset)


