import os
import sys
import stanza
import udax as dx
from pathlib import Path


pos_tagger = stanza.Pipeline(lang="en", processors="tokenize,pos")


def gen_feature_index_tables(amz_ds):
	print(f"Generating feature index tables for {amz_ds}...")
	raw = amz_ds.joinpath("raw")

	nb_f        = raw.joinpath("nb")
	csv_f       = raw.joinpath("csv-train")
	tag_f       = raw.joinpath("tag-train")
	all_uni_f   = raw.joinpath("all_uni.table")
	all_bi_f    = raw.joinpath("all_bi.table")
	turney_bi_f = raw.joinpath("turney_bi.table")

	all_uni_table = {}
	all_bi_table = {}
	turney_bi_table = {}
	
	# in this case, pos is any rating > 3, neg is any <= 3
	pos_ratings = 0
	neg_ratings = 0

	print("Gathering size information...")
	reporter = dx.BlockProcessReporter.file_lines(csv_f, block_size=16)
	stopwatch = dx.Stopwatch()

	print("Processing...")
	stopwatch.start()
	reporter.start()
	with dx.f_open_large_write(tag_f) as tag_h:
		with dx.f_open_large_read(csv_f) as csv_h:
			# c = 0
			for line in csv_h:
				# c += 1
				# if c == 32:
				# 	break
				text, rating, est_rating, est_correct = dx.csv_parseln(line)
				i_rating = int(rating)
				i_est_rating = int(est_rating)

				is_positive = i_rating > 3
				pos_inc = 1 if is_positive else 0
				neg_inc = 0 if is_positive else 1

				pos_ratings += pos_inc
				neg_ratings += neg_inc

				words = text.split()
				pos = [word.upos for sent in pos_tagger(text).sentences for word in sent.words]
				
				# record unigrams
				for word in words:
					if word in all_uni_table:
						record = all_uni_table[word]
						record[0] += pos_inc
						record[1] += neg_inc
						all_uni_table[word] = record
					else:
						n_record = [
							pos_inc,
							neg_inc
						]
						all_uni_table[word] = n_record

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
						record = all_bi_table[bigram]
						record[0] += pos_inc
						record[1] += neg_inc
						all_uni_table[word] = record
					else:
						n_record = [
							pos_inc,
							neg_inc
						]
						all_bi_table[bigram] = n_record

					# turney bigrams:
					if bigram in turney_bi_table:
						record = turney_bi_table[bigram]
						record[0] += pos_inc
						record[1] += neg_inc
						turney_bi_table[bigram] = record
					elif (t_first == "ADJ"  and t_second == "NOUN") or \
						 (t_first == "ADV"  and t_second == "VERB") or \
						 (t_first == "ADV"  and t_second == "ADJ" and t_third != "NOUN") or \
						 (t_first == "ADJ"  and t_second == "ADJ" and t_third != "NOUN") or \
						 (t_first == "NOUN" and t_second == "ADJ" and t_third != "NOUN"):
						n_record = [
							pos_inc,
							neg_inc
						]
						turney_bi_table[bigram] = n_record

					i += 2

				# save tags to an external CSV
				dx.csv_writeln(*pos, stream=tag_h)

				reporter.ping()
			reporter.finish()
	stopwatch.stop()
	print(f"Finished processing in {repr(stopwatch)}")

	print(f"Saving unigram table to {all_uni_f}...")
	with dx.f_open_large_write(all_uni_f) as all_uni_h:
		total_unigrams = 0
		total_pos_unigrams = 0
		total_neg_unigrams = 0
		for posneg in all_uni_table.values():
			total_unigrams += (posneg[0] + posneg[1])
			total_pos_unigrams += posneg[0]
			total_neg_unigrams += posneg[1]
		all_uni_h.write(f"{total_unigrams}\n")
		all_uni_h.write(f"{total_pos_unigrams}\n")
		all_uni_h.write(f"{total_neg_unigrams}\n")
		for uni, posneg in sorted(all_uni_table.items(), key=lambda x: x[1][0] + x[1][1], reverse=True):
			all_uni_h.write(f"{uni} {posneg[0]} {posneg[1]}\n")
	
	print(f"Saving all bigram table to {all_bi_f}...")
	with dx.f_open_large_write(all_bi_f) as all_bi_h:
		total_bigrams = 0
		total_pos_bigrams = 0
		total_neg_bigrams = 0
		for posneg in all_bi_table.values():
			total_bigrams += (posneg[0] + posneg[1])
			total_pos_bigrams += posneg[0]
			total_neg_bigrams += posneg[1]
		all_bi_h.write(f"{total_bigrams}\n")
		all_bi_h.write(f"{total_pos_bigrams}\n")
		all_bi_h.write(f"{total_neg_bigrams}\n")
		for bi, posneg in sorted(all_bi_table.items(), key=lambda x: x[1][0] + x[1][1], reverse=True):
			all_bi_h.write(f"{bi[0]} {bi[1]} {posneg[0]} {posneg[1]}\n")
	
	print(f"Saving turney bigram table to {turney_bi_f}...")
	with dx.f_open_large_write(turney_bi_f) as turney_bi_h:
		total_bigrams = 0
		total_pos_bigrams = 0
		total_neg_bigrams = 0
		for posneg in turney_bi_table.values():
			total_bigrams += (posneg[0] + posneg[1])
			total_pos_bigrams += posneg[0]
			total_neg_bigrams += posneg[1]
		turney_bi_h.write(f"{total_bigrams}\n")
		turney_bi_h.write(f"{total_pos_bigrams}\n")
		turney_bi_h.write(f"{total_neg_bigrams}\n")
		for bi, posneg in sorted(turney_bi_table.items(), key=lambda x: x[1][0] + x[1][1], reverse=True):
			turney_bi_h.write(f"{bi[0]} {bi[1]} {posneg[0]} {posneg[1]}\n")

	print(f"Writing naive bayes probabilities to {nb_f}...")
	with open(nb_f, mode="w") as nb_h:
		tot_ratings = pos_ratings + neg_ratings
		ratio_pos = pos_ratings / tot_ratings
		ratio_neg = neg_ratings / tot_ratings
		nb_h.write(f"{ratio_pos} {ratio_neg}\n")

	print("Ok")

				
if __name__ == "__main__":
	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			gen_feature_index_tables(dataset)


