from mrjob.job import MRJob
import re
import os

WORD_REGEX=re.compile(r"\b\w+\b")
class WordCount(MRJob):
	# saca el nombre del archivo de donde sale la palabra
	def mapper(self, _, line):
		path = os.environ['mapreduce_map_input_file']
		source = path.split("/")[-1]
		words =WORD_REGEX.findall(line)
		for word in words:
			yield ((source, word.lower())[0],1)

	def reducer (self, key_pair, counts):
		yield (key_pair, sum(counts))

if __name__== "__main__":
	WordCount.run()

# python wordcountlab1.py input/sherlock.txt > out.txt