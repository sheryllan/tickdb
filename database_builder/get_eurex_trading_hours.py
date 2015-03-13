#! /usr/bin/env python3

import os,argparse,sys
import re
import csv
import codecs

import urllib.request
from bs4 import BeautifulSoup
from html.parser import HTMLParser

def main():
	# Parse command line
	parser = argparse.ArgumentParser(__file__,description="Eurex trading hours parser")
	parser.add_argument("output",help="output filename", type=str)
	args=parser.parse_args()

	# Download parameters
	urls = [
	"http://www.eurexchange.com/exchange-en/trading/trading-calendar/trading-hours/Trading-hours/58968?frag=138264",
	"http://www.eurexchange.com/exchange-en/trading/trading-calendar/trading-hours/Trading-hours/58968?frag=58942",
	"http://www.eurexchange.com/exchange-en/trading/trading-calendar/trading-hours/Trading-hours/58968?frag=138254",
	"http://www.eurexchange.com/exchange-en/trading/trading-calendar/trading-hours/Trading-hours/58968?frag=656476",
	"http://www.eurexchange.com/exchange-en/trading/trading-calendar/trading-hours/Trading-hours/58968?frag=137646",
	"http://www.eurexchange.com/exchange-en/trading/trading-calendar/trading-hours/Trading-hours/58968?frag=138282",
	"http://www.eurexchange.com/exchange-en/trading/trading-calendar/trading-hours/Trading-hours/58968?frag=138260",
	"http://www.eurexchange.com/exchange-en/trading/trading-calendar/trading-hours/Trading-hours/58968?frag=138240",
	"http://www.eurexchange.com/exchange-en/trading/trading-calendar/trading-hours/Trading-hours/58968?frag=137664"]
	headers={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.76 Safari/537.36"}

	# Open requests
	reqs = ( urllib.request.Request(url=url,headers=headers) for url in urls )
	# Open URL
	files = ( urllib.request.urlopen(req) for req in reqs )
	# Read the HTML page
	contents = ( f.read() for f in files )
	# Parse the HTML page
	soups = [ BeautifulSoup(content) for content in contents ]

	# Extract all tables from the table
	tables = [soup.find_all("table", attrs={"class":"richtextTable dataTable"})[0] for soup in soups]

	# Eurex doesn't use the <th> tag for headers, but the 2 first rows !
	headers = [x.text for x in tables[0].find_all("tr")[0]]
	headers2 = [x.text for x in tables[0].find_all("tr")[1]]
	x = headers[6]
	headers[6] = x+' '+headers2[6]
	headers[7] = x+' '+headers2[7]

	# Retrieve lines from tables
	rows = []
	for table in tables:
		lines = [ l for l in table.find_all("tr") ]
		lines.pop(0) # remove headers
		lines.pop(0)
		for row in lines:
			rows.append([x.text for x in row.find_all("td")])

	# Write CSV file out
	output = codecs.open(args.output, "w", encoding="utf-8")
	writer = csv.writer(output)
	writer.writerow(headers)
	writer.writerows(row for row in rows if row)

if __name__ == '__main__':
	main()
