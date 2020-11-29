
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
       for w in line.decode('utf-8', 'ignore').split():
        vector = line.split(',')
        se=vector[1]
        sa= int(vector[2])
        yield se,sa

    def reducer(self, key, values):
        lista=list(values)
        prom=sum(lista)/len(lista)
        yield key, prom

if __name__ == '__main__':
    MRWordFrequencyCount.run()



























