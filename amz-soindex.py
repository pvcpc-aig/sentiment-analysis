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
from pathlib import Path


def _parse_row(row_str):
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


if __name__ == "__main__":

	data = Path("data")
	for 
