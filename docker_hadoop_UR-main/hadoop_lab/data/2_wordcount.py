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
		path = os.environ['mapreduce_map_input_file']
		source = path.split("/")[-1]
		words =WORD_REGEX.findall(line)
		for word in words:
			if len(word) > 5:
				yield (source, word.lower()),1

	def reducer (self, key_pair, counts):
		yield None, (key_pair, sum(counts))

	def reducer_top(self, _, wc_pairs):
		sorted_pairs = sorted(wc_pairs, key=lambda x: x[1], reverse=True)[:20] 
		for wc_pairs, count in sorted_pairs:
			yield wc_pairs, count
	
	def reducer_byb(self, key_pair, counts):
		source, word = key_pair
		yield source, (word, sum(counts))
	
	def reducer_topbyb(self, source, wc_counts):
		sorted_words = sorted(wc_counts, key=lambda x: x[1], reverse=True)[:3] 
		for word, count in sorted_words:
			yield (source, word), count

if __name__== "__main__":
	WordCount.run()

# python wordcountlab1.py input/sherlock.txt > out.txt