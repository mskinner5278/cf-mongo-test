from random import shuffle, randint

import time
import collections
import monotonic
from matplotlib import pyplot
from pymongo import MongoClient
global result_temp
#connect with client
client = MongoClient('localhost',27017)

#make DB
db=client.insert_embedded


def generate_token_bucket(val_bucket, size=1):
    '''
    produce a shuffled token list consisting of the values in val_bucket duplicated size times
    '''
    token_bucket = []
    for x in range (0, len(val_bucket)):
        for y in range (0, size):
            token_bucket.append(val_bucket[x])
    shuffle(token_bucket)
    return token_bucket

def generate_plot(name, data):
    '''
    name = name of the plot
    data = dictionary with the key is the size of the query result and the value is a list of the query times
    '''
    od = collections.OrderedDict(sorted(data.items()))
    ordered_data = []
    positions = ['0',]
    for k, v in od.iteritems():
        positions.append(k)
        ordered_data.append(v)
    #print ordered_data
    #print positions
    fig = pyplot.figure()
#    ax = fig.add_subplot(111)
    pyplot.boxplot(ordered_data)
    pyplot.title(name)
    pyplot.ylabel('Time(s)')
    pyplot.xlabel('number of channels queried')
    pyplot.xticks(xrange(0, len(positions), 1), positions)
    pyplot.savefig(name)
    pass

def search(name_search_pattern, prop_search_pattern, token):
        global result_temp
        #print "hello"

        #start = time.time()
        #start = monotonic.monotonic_time()

        query_result = db.channels.find(
            {
                "$and": [
                    {
                        "name": {
                            '$regex': name_search_pattern
                        }
                    },
                    {
                        "properties" : {
                            "$elemMatch" : {
                                "name" : prop_search_pattern[0],
                                "value" :  prop_search_pattern[1]
                            }
                    }   }
                ]
            })
        #end = time.time()
        #end = monotonic.monotonic_time()


        #print query_result.explain()
        millis = query_result.explain()["millis"]
        #print query_result.count()
        #print name_search_pattern  ##"SR:C001-PS:2{DP}OK-St"
        #print prop_search_pattern
        #print "time:  "+str((end-start))
        f.write(''.join([name_search_pattern, str(prop_search_pattern), str((millis) * 1000), '\n']))
        #print "token = "+ token
        if(query_result.count() == int(token)):
            if int(token) in result_temp.keys():
                #print "result_temp append"
                result_temp.get(int(token)).append(millis)
            else:
                #print "result_temp enter"
                result_temp[int(token)] = [millis]
        #print
       # print result_temp
        f.write(str(result_temp) +'\n')




if __name__ == '__main__':
    val_bucket = ['1','2','5','10','20','50','100','200','500']
    val_bucket_powers_10 = ['1', '10', '100'] #, '*']
    repetation = 10



    f = open('log_run_20140430_2', 'w')






    '''
    Search for 1,2,5,10,20,50,100,500 channels
    '''
    result_temp = {}
    for token in generate_token_bucket(val_bucket, repetation):
        name_search_pattern = '^SR.*C'+str(randint(1, 100)).zfill(3)+'.*'
        #name_search_pattern = '^SR.*C'+str(1).zfill(3)+'.*'
        prop_search_pattern = ['group'+str(randint(0,9)),token]

        search(name_search_pattern, prop_search_pattern, token)

    result=result_temp
    generate_plot('Performance regular search', result)
    #print "RESULT "
    #print result


    '''
    search for 0 - 100k channels
    '''

    '''
    Compare the performance of searches for a random set of channels and searches for a set of sequential channels
    '''
    result_temp = {}
    for token in generate_token_bucket(val_bucket, repetation):
        name_search_pattern = '^SR.*C'+str(randint(1, 100)).zfill(3)+'.*'
        prop_search_pattern = ['group'+str(randint(0,5)),token]
        search(name_search_pattern,prop_search_pattern, token)

        result_rand=result_temp
    generate_plot('Performance for random set of channels', result_rand)
    #print result_rand





    result_temp = {}
    for token in generate_token_bucket(val_bucket, repetation):
        name_search_pattern = '^SR.*C'+str(randint(1, 100)).zfill(3)+'.*'
        prop_search_pattern = ['group'+str(randint(6,9)),token]
        search(name_search_pattern,prop_search_pattern, token)
    result_ordered=result_temp
    generate_plot('Performance for ordered set of channels', result_ordered)
    #print result_ordered









    '''
    Compare the performance impact of caching
    '''
    result_temp = {}
    for i in val_bucket_powers_10:
        name_search_pattern = '^SR.*C'+str(randint(1, 100)).zfill(3)+'.*'
        prop_search_pattern = ['group'+str(randint(6,9)), i]
        for j in range(0,repetation):
            search(name_search_pattern, prop_search_pattern, i)
    result_cache=result_temp
    f.write(str(result_cache) +'\n')
    generate_plot('Performance of repeating the same query', result_cache)
    #print result_cache
    db.command("collstats", "channels")

    pass
