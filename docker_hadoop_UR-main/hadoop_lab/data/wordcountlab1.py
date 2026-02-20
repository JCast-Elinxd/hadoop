from mrjob.job import MRJob
import re
WORD_REGEX=re.compile(r"\b\w+\b")
class WordCount(MRJob):
	def mapper(self, _, line):
		words =WORD_REGEX.findall(line)
		for word in words:
			yield (word.lower(),1)

	def reducer (self, word, counts):
		yield (word, sum(counts))

if __name__== "__main__":
	WordCount.run()
