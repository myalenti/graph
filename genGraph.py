import random
import threading
import time
import logging
import pymongo
import sys
import timeit
import multiprocessing
import sys
import getopt
import ast
import json
import names
import random
import os
from faker import Factory
from collections import OrderedDict
#from JsonDocuments import JsonDocuments

from pymongo import MongoClient, InsertOne

def usage():
    print "Invalid command line argument"
    print "--minDuns=<number>, --maxDuns=<number> , -D=<dbname>, -C=<collection_name>, -T=<total trees>"
    print " for uri try something like m host:port/database?replicaSet=repset"
    print " if you provide the URI, you don't need to pass the -D option"
    exit()
    
logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)4s) %(levelname)s %(message)s',
                    )
#currentDuns=100000000
#endDuns=100000300
tdb="dnb"
tcoll="tv"
totalTrees=100
process_count=1

try:
    opts, args = getopt.getopt(sys.argv[1:], "C:D:T:p:h:", ["minDuns=", "maxDuns=", "level=", "username=", "password=" ])
    logging.debug("Operation list length is : %d " % len(opts))
except getopt.GetoptError:
    print "You provided invalid command line switches."
    usage()
    exit(2)
    
for opt, arg in opts:
    #print "Tuple is " , opt, arg
    if opt in ("-D"):
        print "Database set to: " , arg
        tdb=str(arg)
    elif opt in ("-C"):
        print "Collection set to: " , arg
        tcoll=str(arg)
    elif opt in ("-T"):
        print "Tree Count set to: " , arg
        totalTrees=int(arg)
    elif opt in ("-p"):
        print "process Count set to: " , arg
        process_count=int(arg)    
    elif opt in ("-h"):
        print "Host Uri: " , arg
        mongoUri=str(arg)
    elif opt in ("--minDuns"):
        print "Min duns set to ", arg
        currentDuns=int(arg)
    elif opt in ("--maxDuns"):
        print "Max Duns set to ", arg
        endDuns=int(arg)
    elif opt in ("--username"):
        print "Username set"
        username=str(arg)
    elif opt in ("--password"):
        print "Password set"
        password=str(arg)
    elif opt in ("--level"):
        print "Log Level set to : ", arg
        arg = arg.upper() 
        if not arg in ("DEBUG", "WARN", "INFO"):
            print "Invalid logging level specified"
            exit(2)
        else:
            logging.getLogger().setLevel(arg)
    elif opt in ("-h"):
        usage()
        exit()
    else:
        usage()
        exit(2)
        
if  mongoUri != None :
    client = MongoClient('mongodb://' + mongoUri, connect=False)
else: 
    client = MongoClient(connect=False)
db = client.get_database(tdb)
db.authenticate(username,password=password,source="admin")
col = db.get_collection(tcoll)

treeDepthMin=1
treeDepthMax=4
siblingCountMin=1
siblingCountMax=6



faker = Factory.create()
faker.seed(os.getpid())

    
#First need to randomly generate a tree depth
#Then need to randomly give a number of members to each layer
#Then generate a topLevel Duns Number
#Then generate all the documents for each level

def buildTreeSkeleton():
    treeDepth = random.randint(treeDepthMin,treeDepthMax)
    countLayer = []
    counter = 0
    while ( treeDepth > counter ) : 
        if counter == 0:
            countLayer.append(1)
            counter += 1
        else:
            countLayer.append(random.randint(siblingCountMin, siblingCountMax))
            counter += 1
    logging.info("Tree Depth %d" % treeDepth)
 
    for a in countLayer:
        logging.info("Array of member count per layer %d " % a)
    return countLayer
####end of buildTreeSkeleton()


def buildTree(treeSkeleton):
    global currentDuns
    depthPosition=0
    tree=OrderedDict()
    treeStr='layer'
    for i in treeSkeleton:
        tree['layer' + str(depthPosition)]=[]
        r=0
        while r < i:
            #tree['layer' + str(depthPosition)].append( str(random.randint(100000000,999999999)))
            tree[treeStr + str(depthPosition)].append( OrderedDict())
            #tree[treeStr + str(depthPosition)][r]['DUNS_NBR']=str(random.randint(100000000,999999999))
            tree[treeStr + str(depthPosition)][r]['DUNS_NBR']=str(currentDuns)
            currentDuns += 1
            tree[treeStr + str(depthPosition)][r]['parent']=setParent(depthPosition, tree)
            r +=1
        depthPosition += 1
    for m,n in tree.iteritems():
        logging.info("Elements %s, %s" % (m ,n ))
    #print tree
    return tree
####end of buildTree()

def setParent( level, tree ):
    #if level 1, then select the only one
    #if not level 1 then randomly select from list above.
    if level == 0 :
        #return tree['layer' + str(level)][0]['DUNS_NBR']
        return None
    else:
        randElement = random.randint(0,len(tree['layer' + str(level - 1)]) - 1)
        return tree['layer' + str(level - 1)][randElement]['DUNS_NBR']
            

def writeTreeToMongo(tree):
    request = []
    for i,j in tree.iteritems(): #Top Level is a dictionary Key with a Value that is an array
        logging.debug( "Top level of tree: %s , %s " % (i, j))
        for r in j: #For all the elements in the array which happen to be dicts.
            logging.debug("Array in top level as key %s " % str(r))
            doc = OrderedDict()
            doc['DUNS_NBR'] = r['DUNS_NBR']
            doc['COMPANY_NAME'] = faker.company()
            doc['GEO_REF_ID'] = random.randint(1,196)
            doc['SUBJ_TYPE_CD'] = random.randint(1000,9999)
            doc['ASSN'] = OrderedDict()
            doc['ASSN']['DUNS_NBR'] = r['parent']
            doc['ASSN']['ASSN_TYPE_CD'] = random.randint(1000,9999)
            #for k,v in r.iteritems(): #Second Level is another dictionary
            #    logging.debug("Second level Objects %s, %s" % (k, v))
            #    doc[k] = v
            
            logging.info("Document : %s" % str(doc))
            request.append(InsertOne(doc))
    logging.debug("full request")
    for i in request:
        logging.debug("%s" % str(i))
    return request
        


def exec_tree(tgtcount, baseDUNS):
    count=0
    global currentDuns
    currentDuns=baseDUNS
    while count < tgtcount:
        treeSkeleton = buildTreeSkeleton()
        tree = buildTree(treeSkeleton)
        #print tree
        req = writeTreeToMongo(tree)
        result = col.bulk_write(req)
        count += 1

tgtcount=totalTrees/process_count

'''
p = multiprocessing.Pool(process_count)
for i in xrange(0,process_count):
    p.apply_async(exec_tree,[tgtcount,])
'''

    
#p.map(exec_tree,[tgtcount])
#p.close()
#p.join()

'''
for i in xrange(1,totalTrees):
    logging.debug("Duns Ranges is %d, %d" % (currentDuns, endDuns))
    if currentDuns < endDuns:
        treeSkeleton = buildTreeSkeleton()
        tree = buildTree(treeSkeleton)
        #print tree
        req = writeTreeToMongo(tree)
        result = col.bulk_write(req)
    else:
        logging.info("Exceeded Duns Number reserve")
        exit()
'''


jobs = []

for i in range(process_count):
    p = multiprocessing.Process(target=exec_tree, args=(tgtcount,(i+1)*100000000,))
    jobs.append(p)
    p.start() 

main_process = multiprocessing.current_process()
logging.debug('Main process is %s %s' % (main_process.name,main_process.pid))

for i in jobs:
    i.join()
        

