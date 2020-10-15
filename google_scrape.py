import urllib.request 
from bs4 import BeautifulSoup


def load_site(url):
	headers = {
		"User-Agent": "Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127"
	}
	req = urllib.request.Request(url=url, headers=headers)
	with urllib.request.urlopen(req) as resp:
		return BeautifulSoup(resp, "html.parser")


def query_google(query, pages=1):
	def _gen_google_search_url(query, page):
		return f"https://www.google.com/search?q={query}&start={page * 10}"

	index_links = []
	for i in range(pages):
		index_page = load_site(_gen_google_search_url(query, i))
		found_indices = False
		for link in index_page.find_all("a"):
			href = link.get("href")
			if href.startswith("/url?q="):
				index_links.append(href.replace("/url?q=", "", 1))
				found_indices = True
			elif found_indices:
				break # ignore the last url pointing to a google login page
	return index_links


for link in query_google("excellent", pages=5):
	try:
		print(load_site(link).get_text())
	except:
		print("uh oh")