import os
import shutil
import numpy.random as npr


DIRPATH = 'uniqueFolder'
DIRS = [DIRPATH,DIRPATH+'1',DIRPATH+'2',DIRPATH+'3',DIRPATH+'4']
f = open('selectors','r').readlines()
f = f+f+f+f
extensions = ['txt','exe','csv','cpp','c','hpp']
searchString = "omeriscool"
scores = []


for dir in DIRS:
    if(os.path.exists(dir)):
        shutil.rmtree(dir)
    os.mkdir(dir)
for elem in npr.choice(f,4000):
    dir = npr.choice(DIRS)
    if(npr.randint(0,2) == 1):
        g = open(os.path.join(dir, '%s.%s'%(elem[:-1],npr.choice(extensions))),'w')
    else:
        fpath = '%s%s.%s' % (elem[:-1],searchString, npr.choice(extensions))
        g = open(os.path.join(dir,fpath), 'w')
        scores.append(fpath)
    g.close()


print('You can run your program with the following args')
dir_path = os.path.dirname(os.path.realpath(__file__))
mappedDirs = map(lambda x: os.path.join(dir_path, x),DIRS)
print('Search %s %s'%(searchString, ' '.join(mappedDirs)))
print(' '.join(scores))
