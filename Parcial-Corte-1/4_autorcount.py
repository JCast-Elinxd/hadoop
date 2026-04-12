from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os

WORD_REGEX=re.compile(r"\b\w+\b")
class WordCount(MRJob):
	def steps(self):
		return [
			MRStep(mapper=self.mapper,
                   reducer=self.reducer),
			MRStep(reducer=self.reducer_top)]

	# saca el nombre del archivo de donde sale la palabra
	def mapper(self, _, line):
		words = line.split(" ")
		for word in words:
			if word == "Author:":
				yield " ".join(words[1:]), 1

	def reducer (self, key_pair, counts):
		yield None, (key_pair, sum(counts))

	def reducer_top(self, _, wc_pairs):
		sorted_pairs = sorted(wc_pairs, key=lambda x: x[1], reverse=True)[:20] 
		for wc_pairs, count in sorted_pairs:
			yield wc_pairs, count
			
if __name__== "__main__":
	WordCount.run()

# python 4_autorcount.py input/* > 5_out.txt