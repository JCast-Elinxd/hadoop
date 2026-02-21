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
		path = os.environ['mapreduce_map_input_file'] # Saca el nombre del archivo de donde sale la palabra
		source = path.split("/")[-1] 
		if source.startswith("olympic"):return # Si hace parte de la base de tweets, no lo procesa

		words =WORD_REGEX.findall(line)
		for word in words:
			if len(word) > 5: # Solo cuento palabras de más de 5 letras
				yield word.lower(),1 # Acá cuento por palabras, sin importar el libro

	def reducer (self, key_pair, counts): # El primer reducer solo suma palabras
		yield None, (key_pair, sum(counts)) 

	def reducer_top(self, _, wc_pairs): # El segundo reducer ordena y saca las 20 palabras más comunes para todos los libros
		sorted_pairs = sorted(wc_pairs, key=lambda x: x[1], reverse=True)[:20] 
		for wc_pairs, count in sorted_pairs:
			yield wc_pairs, count
	
	def mapper_byb(self, _, line):
		path = os.environ['mapreduce_map_input_file'] # Saca el nombre del archivo de donde sale la palabra
		source = path.split("/")[-1] 
		if source.startswith("olympic"):return # Si hace parte de la base de tweets, no lo procesa

		words =WORD_REGEX.findall(line)
		for word in words:
			if len(word) > 5: # Solo cuento palabras de más de 5 letras
				yield (source, word.lower()),1 # Acá si cuento por libros y palabras

	def reducer_byb(self, key_pair, counts): # Suma por libro y palabra
		source, word = key_pair
		yield source, (word, sum(counts))
	
	def reducer_topbyb(self, source, wc_counts): # Ordena por libro y saca las 3 palabras más comunes de cada libro
		sorted_words = sorted(wc_counts, key=lambda x: x[1], reverse=True)[:3] 
		for word, count in sorted_words:
			yield (source, word), count

if __name__== "__main__":
	WordCount.run()

# python 2_wordcount.py input/* > 3_out.txt
# python3 /data/2_wordcount.py -r hadoop --output-dir out hdfs:///user/root/input