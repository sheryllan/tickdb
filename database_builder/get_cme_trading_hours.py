#! /usr/bin/env python3

import os,argparse,sys
import re
import csv
import codecs

import urllib.request
from bs4 import BeautifulSoup
from html.parser import HTMLParser

#class cme_parse(HTMLParser):
#	def handle_starttag(self,tag,attrs):
#
#	def handle_endtag(self,tag):
#
#	def handle_data

def main():
	# Parse command line
	parser = argparse.ArgumentParser(__file__,description="CME trading hours parser")
	parser.add_argument("output",help="output filename", type=str)
	args=parser.parse_args()

	# Set up value for download
	url = "http://www.cmegroup.com/trading_hours/"
	headers={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.76 Safari/537.36"}

	req = urllib.request.Request(url=url,headers=headers)

	# open URL
	f = urllib.request.urlopen(req)

	# Read and parse HTML page
	content = f.read()
	soup = BeautifulSoup(content)

	# extract all tables from the table
	table=soup.find_all("table", attrs={"class":"horRuled"})

	# Get headers (should be the same in all tables so take first)
	headers = [re.sub(r"^ |\n","",header.text) for header in table[1].find_all("th")] 

	# Retrieve lines from tables
	rows = []
	for i in range(0,len(table)):
		for row in table[i].find_all("tr"):
			if len(row)>1:
				line = []
				for val in row.find_all("td"):
					x = val.text
					x = x.encode("ascii","ignore")
					x = re.sub(r"^ |\n","",val.text)
					x = re.sub(r" +"," ",x)
					line.append(x)
				if len(line)>1:
					rows.append(line)

	# Write CSV file out
	output = codecs.open(args.output, "w", encoding="utf-8")
	writer = csv.writer(output)
	writer.writerow(headers)
	writer.writerows(row for row in rows if row)

if __name__ == '__main__':
	main()
