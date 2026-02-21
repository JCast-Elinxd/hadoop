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
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_top)
			]

	def mapper(self, _, line):
		datos = line.split(";")
		ptrn = re.compile(r"^[0-9]+$") # Verifica que el primer campo sea un número (timestamp)
		if ptrn.match(datos[0]):
			date = time.gmtime(int(datos[0])/1000).tm_hour # Convierte el timestamp a hora
			yield date,1

	def reducer (self, date, counts):
		yield None, (date, sum(counts))

	def reducer_top(self, _, date_pairs):
		sorted_pairs = sorted(date_pairs, key=lambda x: x[1], reverse=True)
		for date_pairs, count in sorted_pairs:
			yield date_pairs, count

if __name__== "__main__":
	WordCount.run()

# python 6a_top_hour.py input/olympictweets2016rio.test > 7a_out.txt