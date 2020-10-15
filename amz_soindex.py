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
from nltk.corpus import wordnet as wn
from pathlib import Path

from udax.strutil import surjective_punct_remove


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


post = stanza.Pipeline(lang="en", processors="tokenize,pos")

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


def pos_tag(content):
	"""
	:return
		A list of tuples containing the word and the corresponding
		part of speech tag, i.e.

			[ (word1, pos1), (word2, pos2), ... ]
	"""
	posl = []
	for sentence in post(content).sentences:
		for word in sentence.words:
			posl.append((word.text, word.upos))
	return posl


def extract_phrases(content):
	"""
	Returns all pairs of words within the content string, with
	punctuation omitted, that follow the rules of the given
	table:

		First Word     Second Word    Third Word (not extracted)
		--------------------------------------------------------
		ADJ            NOUN           *
		ADV            ADJ            !NOUN
		ADJ            ADJ            !NOUN
		NOUN           ADJ            !NOUN
		ADV            VERB           *
	
	as specified in Turney, 2002.
	"""
	phrasel = []
	posl = pos_tag(surjective_punct_remove(content))

	i = 0
	while i < len(posl) - 1:
		first = posl[i]
		second = posl[i + 1]
		third_pos = None if len(posl) == i + 2 else posl[i + 2][1]

		if  (first[1] == post.adjective and second[1] == post.noun)                                 or \
			(first[1] == post.adjective and second[1] == post.adjective and third_pos != post.noun) or \
			(first[1] == post.adverb    and second[1] == post.adjective and third_pos != post.noun) or \
			(first[1] == post.adverb    and second[1] == post.verb)                                 or \
			(first[1] == post.noun      and second[1] == post.adjective and third_pos != post.noun):
			phrasel.append((first[0], second[0]))

		# do we add 3 or 2? Maybe both?
		i += 2
	
	return phrasel


def pmi(phrase, compared_to):
	score = 0

	cmp_ssl = wn.synsets(compared_to)
	if cmp_ssl is None or len(cmp_ssl) == 0:
		return 0
	cmp_ss = cmp_ssl[0]

	for word in phrase:
		word_ssl = wn.synsets(word)
		if word_ssl is None or len(word_ssl) == 0:
			continue
		word_ss = word_ssl[0]

		sim = 


if __name__ == "__main__":
	data = Path("data")
	for dataset in data.iterdir():
		is_amazon = dataset.name.startswith("amz")
		if is_amazon:
			print(f"Generating index for {dataset}...")