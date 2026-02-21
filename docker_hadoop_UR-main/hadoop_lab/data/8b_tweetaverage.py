from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
import time

WORD_REGEX=re.compile(r"\b\w+\b")
class WordCount(MRJob):
	def steps(self):
		return [
			MRStep(mapper=self.mapper,
                   reducer=self.reducer)
			]

	def mapper(self, _, line):
		datos = line.split(";")
		if len(datos) < 4: return # Si no hay suficientes datos, se ignora la línea
		chars = len(datos[2]) # Obtiene la longitud del tercer campo
		t_id = datos[1] # Obtiene el ID del tweet (segundo campo)
		yield None, chars

	def reducer (self, _, counts):
		count = 0
		sum_tot = 0
		for i in counts:
			count += 1
			sum_tot += i
		average = sum_tot / count if count > 0 else 0
		yield None, average


if __name__== "__main__":
	WordCount.run()

# python 6b_len_mean.py input/olympictweets2016rio.test > 7b_out.txt