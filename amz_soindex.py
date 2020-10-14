"""
Generates a Semantic Orientation index for a CSV data file
that has been converted by amz-sanitize.py

The CSV is expected to contain rows of the form:

	"<review-text>,<overall-rating>"

	where `review-text` is the string representing the amazon
	review;

	and where `overall-rating` is a number in the range 1 to 5
	representing the rating given by the author of the review.

which is configured in the `amz-sanitize.py` script.
"""
import io
import os
import sys
import stanza
from pathlib import Path


def parse_row(row_str):
	row_list = []

	buf = io.StringIO()
	inquote = False
	escape = False 

	for c in row_str:
		if escape:
			buf.write(c)
			escape = False
			continue

		if c == '\\': 
			escape = True
			continue

		if c == '\"':
			inquote = not inquote
			continue

		if c == ',' and not inquote:
			row_list.append(buf.getvalue())
			buf.close()
			buf = io.StringIO()
			continue

		buf.append(c)
	
	return row_list


post = stanza.Pipeline(lang="en", processors="tokenize,mwt,pos")

# open class words
post.adjective = "ADJ"
post.adverb = "ADV"
post.interjection = "INTJ"
post.noun = "NOUN"
post.proper_noun = "PROPN"
post.verb = "VERB"

# closed class words
post.adposition = "ADP"
post.auxiliary = "AUX"
post.coordinating_conjunction = "CCONJ"
post.determiner = "DET"
post.numeral = "NUM"
post.particle = "PART"
post.pronoun = "PRON"
post.subordinating_conjunction = "SCONJ"

# other
post.punctuation = "PUNCT"
post.symbol = "SYM"
post.other = "X"


def pos_tag(word_list):
	posl = []
	for sentence in post(word_list).sentences:
		for word in sentence.words:
			posl.append(word.upos)
	return posl


def extract_phrases(content):

	


if __name__ == "__main__":
	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			print(f"Generating index for {dataset}...")