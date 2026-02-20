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
			if len(word) > 5:
			    yield ((source, word.lower()),1)
	
    def reducer (self, key_pair, counts):
		yield None, (key_pair, sum(counts))
		
    def reducer_top(self, _, wc_pairs):
        sorted_pairs = sorted(wc_pairs, reverse=True)[:5]
        for count, word in sorted_pairs:
            yield wc_pairs, count

if __name__== "__main__":
	WordCount.run()

# python wordcountlab1.py input/sherlock.txt > out.txt