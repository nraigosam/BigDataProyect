
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
       for w in line.decode('utf-8', 'ignore').split():
        vector = line.split(',')
        em = vector[0]
        se = vector[1]
        yield em,se

    def reducer(self, key, values):
        lista=list(values)
        resultado=len(lista)
        yield key, resultado

if __name__ == '__main__':
    MRWordFrequencyCount.run()



























