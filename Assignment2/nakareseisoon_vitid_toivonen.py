'''
Created on Feb 16, 2016

@author: vitidn
'''
import sys
import itertools
import random

items_str = ""

def getId(item):
    global items_str
    
    if item not in items_str:
        items_str = items_str + item
    
    return items_str.index(item)
    
def getItem(item_id):
    global items_str
    return items_str[item_id]
    
def samplingData(f,proportion):
    """
    \n sampling data from the entire records and store them in array
    \n the final number of sampling lines is estimated to be around (#original_line * proportion)
    \n also, this method will fill in the ID of all item
    \n f - connection stream to the data file
    \n proportion - ratio of the sampling lines
    \n << return >>
    \n [frozenset([...]),frozenset([...]),...], each element is a set of item appeared in each line
    """
    sampling_data = []
    
    for line in f:
        items = line.replace("\n","").split(",")
        #fill in item's ID
        item_ids = [getId(x) for x in items]
        #we compute the probability when reading each line 
        #because we can't afford to reading the entire lines first just to get a number of lines and use it later(it will requires 2 passes instead)
        if(random.uniform(0,1) <= proportion):
            item_set = frozenset(item_ids)
            sampling_data.append(item_set)
    
    return sampling_data
    
def constructCandidateSets(itemsets):
    """
    \n construct immediate supersets from the current itemsets 
    \n itemsets - format: [frozenset([...]),frozenset([...]),...]
    \n << return >>
    \n final_supersets - construced immediate supersets, format: [frozenset([...]),frozenset([...]),...]
    """
    #no itemsets with size n-1   
    if(len(itemsets)==0):
        return []
        
    size = len(itemsets[0])
    #joining with itself,each itemsets has size n
    #we need supersets with size n+1
    #n(AUB) = n(A) + n(B) - n(A^B)
    #n+1 = n + n - (n-1)
    supersets = [x | y for x in itemsets for y in itemsets if len(x.intersection(y)) == (size-1) ]
    #remove possible duplicate supersets
    supersets = frozenset(supersets)
    final_supersets = []
    #check that immediate subsets of each superset must also appear(also be frequent)
    for superset in supersets:
        is_valid = True
        for subset_candidate in itertools.combinations(superset,size):
            if frozenset(subset_candidate) not in itemsets:
                is_valid = False
                break
        if(is_valid):
            final_supersets.append(superset)
    return final_supersets

def constructNegativeBorder(list_freq_itemsets):
    """
    \n list_freq_itemsets - LIST of frequent itemsets of size 1...n
    \n e.g. [[frozentset(i),...],[frozentset(i,j),...],[frozentset(i,j,k),...],...]
    \n << return >>
    \n list_neg_itemsets - in the same format as list_freq_itemsets
    \n Noted - the empty list will be filtered out
    """
    global items_str
    
    #in a rare case where sampling data don't obtain any frequent itemsets(even for singletons), just return the singleton itemsets back
    if(len(list_freq_itemsets)==0):
        return [frozenset([x]) for x in range(0,len(items_str))]
    
    list_neg_itemsets = []
    
    for i in range(0,len(list_freq_itemsets)):
        if(i==0):
            #to get a negative border of size = 1, all possible itemsets of size 1 is all singletons that appeared in the mapped string items_str
            possible_freq_itemsets_n = [frozenset([x]) for x in range(0,len(items_str))]
        else:
            #to get a negative border of size > 1, we can use all frequent itemsets of size n-1 for construction
            freq_itemsets_n_1 = list_freq_itemsets[i-1]
            possible_freq_itemsets_n = constructCandidateSets(freq_itemsets_n_1)
        
        #now, extract only those frequent itemsets that are not frequent in the sample count
        #apply frozenset(...) because each element is a list
        neg_itemsets = frozenset(possible_freq_itemsets_n) - frozenset(list_freq_itemsets[i])
        #turn the output frozentset back to list...
        neg_itemsets = list(neg_itemsets)
        list_neg_itemsets.append(neg_itemsets)
    
    #finally, construct the negative border of size n+1   
    list_neg_itemsets.append(constructCandidateSets(list_freq_itemsets[len(list_freq_itemsets)-1]))
    #filter out empty lists
    list_neg_itemsets = [x for x in list_neg_itemsets if len(x) > 0]
     
    return list_neg_itemsets

def apriori_n_pass(sampling_data,threshold,freq_itemsets,filter_size):
    """
    \n freq_itemsets - the list of all frequent itemsets with size filter_size-1
    \n                 format: [frozenset([...]),frozenset([...]),...]
    \n                 Noted: this value will be None in the first round(filter_size = 1) 
    \n << return >>
    \n frequent itemsets of size filter_size
    """
    
    #if it is the 1st pass, no freq_itemsets is sent
    if(filter_size == 1):
        singleton_counts = []
        
        for fs in sampling_data:
            for index in list(fs):
                if(index == len(singleton_counts)):
                    singleton_counts.append(1)
                else:
                    singleton_counts[index] = singleton_counts[index] + 1
        
        freq_itemsets = [frozenset([x]) for x in range(0,len(singleton_counts)) if singleton_counts[x] >= threshold]
            
        return freq_itemsets
    
    #if it is the subsequent pass, construct candidate_itemsets with size filter_size
    
    #this is candidate itemsets with size filter_size
    #format: format: [frozenset([...]),frozenset([...]),...]
    candidate_itemsets = constructCandidateSets(freq_itemsets)
    
    if(len(candidate_itemsets)==0):
            raise Exception("TerminateAlgorithm")
    
    #store the value of each candidate frequent itemset by Triples method
    dict_candidate_itemsets = {}
    #change fronzenset(i,j,...) to fronzenset(i,j,...):<count>, initialize each count as 0
    for candidate_filter_itemset in candidate_itemsets:
        dict_candidate_itemsets[candidate_filter_itemset] = 0
    
    #do the actual count of each candidate itemset
    for fs in sampling_data:
        for candidate_filter_itemset in dict_candidate_itemsets:
            if(candidate_filter_itemset.issubset(fs)):
                dict_candidate_itemsets[candidate_filter_itemset] = dict_candidate_itemsets[candidate_filter_itemset] + 1

    #filter candidate_itemsets and now we get frequent itemsets of size filter_size 
    true_freq_itemsets = [itemset for itemset,count in dict_candidate_itemsets.items() if count >= threshold]
   
    return true_freq_itemsets

def toivonen_full_pass(f,threshold,list_freq_itemsets,list_neg_itemsets):
    
    #e.g. {frozenset([i]):<count>,frozenset([j]):<count>,frozenset([i,j]):<count>,...}
    dict_freq_itemsets = {}
    dict_neg_itemsets = {}
    
    for line in f:
        items = line.replace("\n","").split(",")
        item_ids = [getId(x) for x in items]
        item_set = frozenset(item_ids)
        #check for false positive frequent itemsets
        for freq_itemsets in list_freq_itemsets:
            for fs in freq_itemsets:
                if(fs.issubset(item_set)):
                    if fs in dict_freq_itemsets:
                        dict_freq_itemsets[fs] = dict_freq_itemsets[fs] + 1
                    else:
                        dict_freq_itemsets[fs] = 1
        #check for negative border itemsets
        for neg_itemsets in list_neg_itemsets:
            for fs in neg_itemsets:
                if(fs.issubset(item_set)):
                    if fs in dict_neg_itemsets:
                        dict_neg_itemsets[fs] = dict_neg_itemsets[fs] + 1
                    else:
                        dict_neg_itemsets[fs] = 1
    
    #if it turned out that at least one of the item in negative border is frequent, rerun the algorithm                    
    for neg_item,count in dict_neg_itemsets.iteritems():
        if(count >= threshold):
            raise Exception("RerunAlgorithm")
        
    #now, we filter out the false-positive frequent itemsets
    list_freq_itemsets = []
    #e.g. {1:[frozenset([0]),frozenset([1])],2:[frozenset([0,1]),...]}
    list_dict = {}
    while(True):
        try:    
            itemset,count = dict_freq_itemsets.popitem()
            if(count >= threshold):
                set_size = len(itemset)
                if(set_size not in list_dict):
                    list_dict[set_size] = []
                list_dict[set_size].append(itemset)
                
        except KeyError:
            #dict_freq_itemsets is now empty
            break
    #reconstruct the list_freq_itemsets
    for i in range(1,len(list_dict)+1):
        freq_itemsets = list_dict.pop(i,None)
        list_freq_itemsets.append(freq_itemsets)
        
    return list_freq_itemsets
        
def toivonen(filename,frequent_threshold,proportion,adjust_threshold):
    """
    \n << return >>
    \n list_freq_itemsets - LIST of frequent itemsets
    \n e.g. [[frozentset(i),...],[frozentset(i,j),...],[frozentset(i,j,k),...],...]
    \n number_of_iteration - <int>
    """
    global items_str
    
    is_nagative_border_freq = True
    list_freq_itemsets = []
    num_iteration = 0
    while is_nagative_border_freq:
        num_iteration = num_iteration + 1
        #reset the mapping table
        items_str = ""
        sampling_data = samplingData(open(filename),proportion)
        filter_size = 1
        freq_itemsets = None
        #list_freq_itemsets - [[frozentset(i),...],[frozentset(i,j),...],[frozentset(i,j,k),...],...]
        list_freq_itemsets = []
        while True:
            try:
                freq_itemsets = apriori_n_pass(sampling_data, round(frequent_threshold*proportion*adjust_threshold), freq_itemsets, filter_size)
                list_freq_itemsets.append(freq_itemsets)
                filter_size = filter_size + 1
            except:
                break
        #construct the negative border itemsets    
        list_neg_itemsets = constructNegativeBorder(list_freq_itemsets)
        
        try:
            list_freq_itemsets = toivonen_full_pass(open(filename), frequent_threshold, list_freq_itemsets, list_neg_itemsets)
            is_nagative_border_freq = False
        except :
            pass
    return (list_freq_itemsets,num_iteration)
    
if __name__ == "__main__":
    filename = sys.argv[1]
    frequent_threshold = int(sys.argv[2])
    proportion=0.4
    
    result = toivonen(filename, frequent_threshold,proportion,adjust_threshold=0.8)
    list_freq_itemsets,iteration_count = result[0],result[1]
    
    print(iteration_count)
    print(proportion)
    for freq_itemsets in list_freq_itemsets:
        printed_list = sorted([sorted(map(getItem,x)) for x in freq_itemsets])
        print(printed_list)    