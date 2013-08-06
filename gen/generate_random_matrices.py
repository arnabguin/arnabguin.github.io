#!/usr/bin/python

import threading
import sys 
import os
import subprocess 

from numpy import array,uint8,savetxt
from numpy.random import random_integers

class randomMatrixGenerator(threading.Thread):
    def __init__ (self,i,lb,ub,size,odir):
        threading.Thread.__init__(self)
        self.i = i
        self.lb = lb
        self.ub = ub
        self.size = size  
        self.fh = None
        self.odir = os.getcwd() + '/' + odir

    def printout(self,matrix):
        nr = len(matrix)
        nc = len(matrix[0])

        if verbose:
            print "Matrix create thread %d : nr=%d,nc=%d" % (self.i,nr,nc)

        for r in range(len(matrix)):
            for c in range(nc):
                if c:
                    self.fh.write(" " + str(matrix[r][c])),
                else:
                    self.fh.write(str(matrix[r][c])),
            self.fh.write("\n")
        sys.stdout.flush()

    def run(self):
        try:
            os.mkdir(self.odir)
        except OSError,e:
            pass
        except Exception,e:
            raise Exception(e.value)
        finally:
            print "Matrix " + str(self.i) + ": dimension = " + str(self.size)

        with open(self.odir + '/part-' + '{0:05b}'.format(self.i), 'w') as self.fh:
            self.printout(random_integers(self.lb,self.ub,self.size))


if __name__ == "__main__":

    if len(sys.argv) is not 3:
        print "Usage generate_random_matrices.py <dimension file> <output directory>"
        exit(1)

    numMatrices = 0
    matrices = []

    lastc = 0 
    verbose = 1

    outputdir = sys.argv[2]

    with open(sys.argv[1]) as inputfile:
        (lowerbound,upperbound) = [ int(p) for p in inputfile.readline().split(':') ]
        if verbose:
            print "U=%d,L=%d" % (lowerbound,upperbound)
            print 

        numMatrices = 0

        genr = []
        for line in inputfile:
            (r,c) = [ int(p) for p in line.split(',') ]
            if r != lastc and len(genr) > 0:
                raise Exception("Matrices %d and %d are not compatible for multiplication (line %d)" % (numMatrices, numMatrices + 1, numMatrices + 2))

            genr.append(randomMatrixGenerator(numMatrices,lowerbound,upperbound,(r,c),outputdir))

            lastc = c
            numMatrices = numMatrices + 1
  
        for g in genr:
            g.start()
        for g in genr:
            g.join()

    print "Output matrix in %s\n" % outputdir 


