import io
import os
import sys
import joblib
from nltk.corpus import sentiwordnet as swn
from sklearn.svm import LinearSVC
from pathlib import Path

from udax.strutil import wordify_wordlist


# When reading the raw dataset, this will skip a potential header
# if set to true.
skip_first_csv_line = True

# The number of features per vector in a sample.
feature_vector_size = 10

# The number of lines to process before emitting a status report
# to the user when processing large files.
lines_per_status_report = 2 ** 16

# A read buffer size, in bytes, of 64MB
max_read_buffer = 2 ** 26

# A write buffer size, in bytes, of 32MB
max_write_buffer = 2 ** 25


# TODO(max): put these common things in udax
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

        buf.write(str(char))

    left_over = buf.getvalue()
    if len(left_over) > 0:
        row_cell_list.append(left_over)

    return row_cell_list


def load_n_most_freq_words(n, table_f):
	words = []
	with table_f.open(mode="r") as wft_h:
		for i, line in enumerate(wft_h):
			pair = line.split()
			word = pair[0]
			words.append(word)
			if i + 1 == n:
				break
	return words


def so_calc_binary(synset_seen, word_list):
	pos_score = 0
	neg_score = 0
	for word in word_list:
		if word in synset_seen:
			score = synset_seen[word]
			exists = score[0]
			if exists:
				pos_score += score[1]
				neg_score += score[2]
			continue

		synsets = list(swn.senti_synsets(word))
		if synsets is None or len(synsets) == 0:
			synset_seen[word] = (False, 0, 0)
			continue

		synset = synsets[0]
		pos_score += synset.pos_score()
		neg_score += synset.neg_score()

		synset_seen[word] = (True, synset.pos_score(), synset.neg_score())

	if pos_score > neg_score:
		return 1
	if neg_score > pos_score:
		return -1
	return 0


def gen_samples(dataset, raw_f, out_f):
	from math import ceil

	word_freq_table_f = dataset.joinpath("dict")
	words_most_common = load_n_most_freq_words(feature_vector_size, word_freq_table_f)
	synset_seen = dict()

	print(f"Generating feature set for {raw_f} into {out_f}...")
	with out_f.open(mode="w", buffering=max_write_buffer) as out_h:
		with raw_f.open(mode="r", buffering=max_read_buffer) as raw_h:

			# Gather size information for user input
			print(f"Gathering size information for {raw_f}...")
			max_lines = 0
			max_blocks = 0
			if skip_first_csv_line:
				raw_h.readline()
			for line in raw_h:
				max_lines += 1
			max_blocks = int(ceil(max_lines / lines_per_status_report))
			
			raw_h.seek(0)

			# Process the raw csv data into a set of vector features
			print(f"Converting raw data to feature set...")
			n_line = 0
			n_block = 0
			if skip_first_csv_line:
				raw_h.readline()
			for line in enumerate(raw_h):
				parsed_line = parse_line(line)

				word_list = wordify_wordlist(parsed_line[0])
				sample_vector = [ 0 ] * (feature_vector_size + 1)
				sample_vector[-1] = so_calc_binary(synset_seen, word_list)

				for word in word_list:
					try:
						ind = words_most_common.index(word)
						if ind != -1:
							sample_vector[ind] += 1
					except: # ignore this useless exception
						pass
				
				out_line = ','.join([str(x) for x in sample_vector])
				out_h.write(f"{out_line}\n")

				n_line += 1
				if n_line % lines_per_status_report == 0:
					n_block += 1
					print(f"[%5.1f%%] Processed {n_block}/{max_blocks}..." % (100 * n_block / max_blocks))
			
			if n_line % lines_per_status_report > 0:
				n_block += 1
				print(f"[100.0%] Processed {max_blocks}/{max_blocks}")


def svm_eval(amz_ds):
	raw = amz_ds.joinpath("raw")
	svm = amz_ds.joinpath(f"svm-{feature_vector_size}")
	if not svm.exists():
		svm.mkdir()

	csv_train = raw.joinpath("csv-train")
	csv_test = raw.joinpath("csv-test")

	csv_vectors_train = svm.joinpath("csv-train-vectors")
	csv_vectors_test = svm.joinpath("csv-test-vectors")
	results_file = svm.joinpath("svm.results")
	model_file = svm.joinpath("svm.bin")

	# Generate the training feature vector set if needed.
	if not csv_vectors_train.exists():
		gen_samples(amz_ds, csv_train, csv_vectors_train)
	
	# Generate the testing feature vector set if needed.
	if not csv_vectors_test.exists():
		gen_samples(amz_ds, csv_test, csv_vectors_test)
	
	# Create and train a model if a save does not already exist.
	if not model_file.exists():
		print("No saved model exists; creating...")
		model = LinearSVC()

		print("Loading training information...")
		x_train = []
		y_train = []
		with csv_vectors_train.open(mode="r", buffering=max_read_buffer) as train_h:
			for line in train_h:
				sample = line.split(',')
				x_train.append([int(i) for i in sample[:-1]])
				y_train.append(int(sample[-1]))
		
		print("Training model...")
		model.fit(x_train, y_train)

		print(f"Saving model to {model_file}")
		joblib.dump(model, model_file)
	
	print("Loading model...")
	model = joblib.load(model_file)

	print("Loading testing information...")
	x_test = []
	y_test = []
	with csv_vectors_test.open(mode="r", buffering=max_read_buffer) as test_h:
		for line in test_h:
			sample = line.split(',')
			x_test.append([int(i) for i in sample[:-1]])
			y_test.append(int(sample[-1]))
	
	print("Evaluating model...")
	acc = model.score(x_test, y_test)

	print("Evaluation score: accuracy %1.f" % (100 * acc))
	print(f"Saving results to {results_file}...")

	with results_file.open(mode="w") as rf_h:
		rf_h.write(f"acc = {acc}\n")


if __name__ == "__main__":
	# verify settings
	if feature_vector_size <= 0:
		print("feature_vector_size must be > 0")
		sys.exit(1)

	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			svm_eval(dataset)