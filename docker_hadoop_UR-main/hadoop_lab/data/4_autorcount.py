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

	def mapper(self, _, line): 
		path = os.environ['mapreduce_map_input_file']
		source = path.split("/")[-1]
		if source.startswith("olympic"):return # Si hace parte de la base de tweets, no lo procesa
		
		words = line.split(" ")
		for word in words:
			if word == "Author:": # Si la palabra es Author, se asume que el siguiente string es el autor del libro, y se cuenta por autor
				yield " ".join(words[1:]), 1 

	def reducer (self, key_pair, counts):
		yield None, (key_pair, sum(counts)) # Cuenta por autor, sin importar el libro

	def reducer_top(self, _, wc_pairs):
		sorted_pairs = sorted(wc_pairs, key=lambda x: x[1], reverse=True)[:20] # Saca los 20 autores más comunes
		for wc_pairs, count in sorted_pairs:
			yield wc_pairs, count
			
if __name__== "__main__":
	WordCount.run()

# python 4_autorcount.py input/* > 5_out.txt
# python3 /data/4_autorcount.py -r hadoop --output-dir out hdfs:///user/root/input