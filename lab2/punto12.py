
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
       for w in line.decode('utf-8', 'ignore').split():
        vector = line.split(',')
        em =vector[0]
        sa = int(vector[2])
        yield em,sa

    def reducer(self, key, values):
        lista=list(values)
        prom=sum(lista)/len(lista)
        yield key, prom

if __name__ == '__main__':
    MRWordFrequencyCount.run()



























