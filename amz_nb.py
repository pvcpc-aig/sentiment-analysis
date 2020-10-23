import os
import sys
import udax as dx
from pathlib import Path


def ldtable(path, words=1, max_entries=2048):
	entries = 0
	table = {}
	h_tot = 0
	h_pos = 0
	h_neg = 0
	with dx.f_open_large_read(path) as handle:
		h_tot = int(handle.readline())
		h_pos = int(handle.readline())
		h_neg = int(handle.readline())
		for ln in handle:
			dat = ln.split()
			if words == 1:
				word, pos, neg = dat
				table[word] = ( int(pos), int(neg) )
			else:
				words = tuple(dat[:-2])
				pos = dat[-2]
				neg = dat[-1]
				table[words] = ( int(pos), int(neg) )
			entries += 1
			if entries == max_entries:
				break
	return table, h_tot, h_pos, h_neg


def nb(amz_ds):
	raw = amz_ds.joinpath("raw")

	csv_f       = raw.joinpath("csv-test")
	nb_f        = raw.joinpath("nb")
	nb_report_f = raw.joinpath("nb.report")
	all_uni_f   = raw.joinpath("all_uni.table")
	all_bi_f    = raw.joinpath("all_bi.table")
	turney_bi_f = raw.joinpath("turney_bi.table")
	
	print("Gathering size information...")
	reporter = dx.BlockProcessReporter.file_lines(csv_f, block_size=256)
	stopwatch = dx.Stopwatch()

	all_uni_table, all_uni_tot, all_uni_pos, all_uni_neg = ldtable(all_uni_f)
	all_bi_table, all_bi_tot, all_bi_pos, all_bi_neg = ldtable(all_bi_f, words=2)
	turney_bi_table, turney_bi_tot, turney_bi_pos, turney_bi_neg = ldtable(turney_bi_f, words=2)

	percent_pos = 0
	percent_neg = 0
	with nb_f.open(mode="r") as nb_h:
		percent_pos, percent_neg = [float(x) for x in nb_h.readline().split()]
	
	# p(neg|x) = p(neg) * p(x|neg) / p(x)
	# p(pos|x) = p(pos) * p(x|pos) / p(x)

	uni_predict_correct = 0
	uni_predict_tot = 0 
	uni_predict_acc = 0

	bi_predict_correct = 0
	bi_predict_tot = 0
	bi_predict_acc = 0

	turney_predict_correct = 0
	turney_predict_tot = 0
	turney_predict_acc = 0

	print("Processing...")
	stopwatch.start()
	reporter.start()
	with dx.f_open_large_read(csv_f) as csv_h:
		for line in csv_h:
			text, rating, est_rating, est_correct = dx.csv_parseln(line)
			i_rating = int(rating)
			i_est_rating = int(rating)
			words = text.split()

			p_pos = percent_pos
			p_neg = percent_neg

			# unigram naive bayes
			for word in words:
				if word not in all_uni_table:
					continue
				info = all_uni_table[word]
				pos_count, neg_count = info

				p_pos *= pos_count / all_uni_pos
				p_neg *= neg_count / all_uni_neg

			if (p_pos - p_neg > 0 and i_rating > 3) or (p_pos - p_neg < 0 and i_rating <= 3): # predicted correct
				uni_predict_correct += 1
			uni_predict_tot += 1

			
			p_pos = percent_pos
			p_neg = percent_neg

			# all bigrams
			i = 0
			while i < len(words) - 1:
				first = words[i]
				second = words[i + 1]
				bigram = (first, second)
				i += 2

				if bigram not in all_bi_table:
					continue

				info = all_bi_table[bigram]
				pos_count, neg_count = info
				
				p_pos *= pos_count / all_bi_pos
				p_neg *= neg_count / all_bi_neg

			if (p_pos - p_neg > 0 and i_rating > 3) or (p_pos - p_neg < 0 and i_rating <= 3): # predicted correct
				bi_predict_correct += 1
			bi_predict_tot += 1


			p_pos = percent_pos
			p_neg = percent_neg

			# turney bigrams
			i = 0
			while i < len(words) - 1:
				first = words[i]
				second = words[i + 1]
				bigram = (first)
				i += 2
				
				if bigram not in turney_bi_table:
					continue

				info = turney_bi_table[bigram]
				pos_count, neg_count = info

				p_pos *= pos_count / turney_bi_pos
				p_neg *= neg_count / turney_bi_neg

			if (p_pos - p_neg > 0 and i_rating > 3) or (p_pos - p_neg < 0 and i_rating <= 3): # predicted correct
				turney_predict_correct += 1
			turney_predict_tot += 1

			reporter.ping()
	
	reporter.finish()
	stopwatch.stop()
	print(f"Done in {repr(stopwatch)}")
	
	uni_predict_acc = uni_predict_correct / uni_predict_tot
	bi_predict_acc = bi_predict_correct / bi_predict_tot
	turney_predict_acc = turney_predict_correct / turney_predict_tot

	print(f"Saving naive bayes evaluation results to {nb_report_f}...")
	with nb_report_f.open(mode="w") as nb_report_h:
		nb_report_h.write("unigram accuracy: %.3f\n" % (100 * uni_predict_acc))
		nb_report_h.write("all bigram accuracy: %.3f\n" % (100 * bi_predict_acc))
		nb_report_h.write("turney bigram accuracy: %.3f\n" % (100 * turney_predict_acc))


if __name__ == "__main__":
	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			nb(dataset)
