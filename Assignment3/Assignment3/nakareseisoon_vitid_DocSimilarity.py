import sys
import glob
import itertools

def shingling(folder_path,k,type_shingles):
    """
    \n result_list - contains the list of a tuple of file_name and its shingles set 
    \n               format [(txt_name,set(...)),(txt_name,set(...)),...]
    """
    result_list = []
    txt_files = glob.glob(folder_path+"/*.txt")
    txt_files.sort(reverse=False)
    for file in txt_files:
        with open(file,'r') as f:
            line = f.read()
            if(type_shingles == "char"):
                list_token = list(line)
            else:
                list_token = line.strip().split(" ")
            
            shingle_set = set()    
            for i in range(0,len(list_token)-k+1):
                shingle = list_token[i:(i+k)]
                shingle = "".join(shingle)
                shingle_set.add(shingle)
                
            result_list.append((file,shingle_set))
    
    return result_list

def minHashing(num_hash,all_shingle_list,doc_shingle_list):
    """
    \n all_shingle_list - contains all sorted shingles in the corpus, format: ['a','b',...]
    \n doc_shingle_list - contains the list of a tuple of file_name and its shingles set
    \n                    format [(txt_name,set(...)),(txt_name,set(...)),...]
    \n return doc_min_hash_list - contains the list of a tuple of file_name and its min-hash signature
                          format [(txt_name,[h_A1,h_A2,...]),(txt_name,[h_B1,h_B2,...]),...]
    """
    num_shingle = len(all_shingle_list)
    #hash_table - [
    #               [h1,h2,...,h_n],
    #               [h1,h2,...,h_n],
    #               [h1,h2,...,h_n],
    #               ...
    #             ]
    #number of rows = num_shingle
    #number of columns = num_hash
    hash_table = [[-1 for x in range(0,num_hash)] for x in range(0,num_shingle)]
    
    #construct the hash value for each row for all num_hash
    for i in range(0,num_shingle):
        #compute the hash of the current row for shingle i th
        for hash_index in range(1,num_hash+1):
            h = ( (hash_index * i) + 1 ) % num_shingle
            hash_table[i][hash_index-1] = h
    #now, calculate the mini-hash
    #construct a signature-table
    #signature_table - [
    #                    [h_A,h_B,h_C],
    #                    [h_A,h_B,h_C],
    #                    [h_A,h_B,h_C],
    #                    ...
    #                  ]
    signature_table = [[-1 for x in range(0,len(doc_shingle_list))] for x in range(0,num_hash)]
    #looping from the first shingle to the last
    for i in range(0,num_shingle):
        shingle = all_shingle_list[i]
        for j in range(0,len(doc_shingle_list)):
            doc_shingle = doc_shingle_list[j]
            doc,shingle_set = doc_shingle[0],doc_shingle[1]
            if(shingle in shingle_set):
                #the corresponding entry in chracteristic matrix is "1"
                #re-calculate the corresponding signature
                signatures = [row[j] for row in signature_table]
                current_hash = hash_table[i]
                for k in range(len(signatures)):
                    if signatures[k] == -1 or signatures[k] > current_hash[k]:
                        signatures[k] = current_hash[k]
                #re-assign signatures to the signature_table
                for k in range(len(signatures)):
                    signature_table[k][j] = signatures[k]
    
    doc_min_hash_list = []
    #construct the min-hash signature from the signature_table
    for i in range(0,len(doc_shingle_list)):
        doc_shingle = doc_shingle_list[i]
        doc,shingle_set = doc_shingle[0],doc_shingle[1]
        min_hash_signature = [row[i] for row in signature_table]
        doc_min_hash_list.append((doc,min_hash_signature))
        
    return doc_min_hash_list

def lsh(num_hash,threshold,doc_min_hash_list,BUCKET_SIZE = 10000):
    """
    \n doc_min_hash_list - contains the list of a tuple of file_name and its min-hash signature
    \n                     format [(txt_name,[h_A1,h_A2,...]),(txt_name,[h_B1,h_B2,...]),...]
    \n return : 
    \n candidate_pairs - contained candidated pairs of document names, sorted, format: [[A,B],[A,C],[B,C]...]                   
    """
    br = computeOptimalBR(num_hash, threshold)
    b,r = br[0],br[1]
    
    #{frozenset(A,B),frozenset(A,C),frozenset(B,C),...}
    candidate_pairs = set()
    
    for i in range(0,b):
        #{hash_value0:set(A,B),hash_value1:set(C,D,E,F),...}
        #clear the bucket for a new band
        bucket = {}
        first_index = i*r
        last_index = i*r + (r - 1)
        for doc_min_hash in doc_min_hash_list:
            doc,min_hash = doc_min_hash[0],doc_min_hash[1]    
            signature_in_band = min_hash[first_index:(last_index+1)]
            hash_value = sum([(x+1)*signature_in_band[x] for x in range(len(signature_in_band))]) % BUCKET_SIZE
            #hash_value = tuple(signature_in_band)
            if hash_value not in bucket:
                bucket[hash_value] = set()
            bucket[hash_value] = bucket[hash_value].union(set([doc]))
        #we got the bucket for the current band
        #populate all pairs that belong to the buckets that have more than 1 member
        for hash_value,doc_set in bucket.iteritems():
            if len(doc_set) <= 1:
                continue
            for pair_docs in list(itertools.combinations(doc_set,2)):
                candidate_pairs.add(frozenset(pair_docs))
                
    #re-order the result and turn it to a list
    candidate_pairs = list(candidate_pairs)
    candidate_pairs = [list(x) for x in candidate_pairs]
    
    for x in candidate_pairs:
        x.sort()
        
    candidate_pairs.sort()
    return candidate_pairs

def displayJaccardSimilarity(tuple_list):
    """
    \n tuple_list - format: [(txt_name,set(...)),(txt_name:set(...)),...]
    """
    for i in range(0,len(tuple_list)-1):
        file_a,set_a = tuple_list[i][0],tuple_list[i][1]
        for j in range(i+1,len(tuple_list)):
            file_b,set_b = tuple_list[j][0],tuple_list[j][1]
            js = (len(set_a.intersection(set_b)) + 0.0)/len(set_a.union(set_b))
            print("Jaccard Similarity between {0} and {1}:{2}".format(file_a,file_b,js))

def displayEstimatedJaccardSimilarity(tuple_list,num_hash):
    """
    \n tuple_list - format: [(txt_name,[h_A1,h_A2,...,h_An]),(txt_name:[h_B1,h_B2,...,h_Bn]),...]
    """
    for i in range(0,len(tuple_list)-1):
        file_a,list_a = tuple_list[i][0],tuple_list[i][1]
        for j in range(i+1,len(tuple_list)):
            file_b,list_b = tuple_list[j][0],tuple_list[j][1]
            agreed_list = [ 1 if list_a[i] == list_b[i] else 0 for i in range(len(list_a))]
            js = (sum(agreed_list) + 0.0)/num_hash
            print("Jaccard Similarity between {0} and {1}:{2}".format(file_a,file_b,js))

def computeOptimalBR(num_hash,s):
    """
    \n s - value between [0,1]
    \n return: tuple(b,r)
    """
    best_threshold = -1
    best_b_r = (-1,-1)
    for i in range(1,num_hash+1):
        if(num_hash % i == 0):
            b = i
            r = num_hash / b
            threshold = (1.0/b) ** (1.0/r)
            print("({0},{1}) has absolute difference:{2}".format(b,r,abs(threshold - s )))
            if(best_threshold == -1 or abs(threshold - s ) < abs(best_threshold - s )):
                best_threshold = threshold
                best_b_r = (b,r)
    #best_b_r = (50,3)
    #best_b_r = (100,1) 
    print("Best (B,R)" + str(best_b_r))   
    return best_b_r

#For testing:            
#python nakareseisoon_vitid_DocSimilarity.py Docs 5 char 120 0.35
#python nakareseisoon_vitid_DocSimilarity.py Docs 2 word 30 0.35
if __name__ == "__main__":
    folder_path = sys.argv[1]
    k = int(sys.argv[2])
    #type_shingles - "char"/"word"
    type_shingles = sys.argv[3]
    num_hash = int(sys.argv[4])
    threshold = float(sys.argv[5])
    
    result_list = shingling(folder_path, k, type_shingles)
    
    all_shingle_set = set()
    for t in result_list:
        file_name,shingle_set = t[0],t[1]
        print("No of Shingles in File {0}:{1}".format(file_name,len(shingle_set)))
        all_shingle_set = all_shingle_set.union(shingle_set)
        
    all_shingle_list = list(all_shingle_set)
    all_shingle_list.sort()
        
    displayJaccardSimilarity(result_list)
    print("")
    
    doc_min_hash_list = minHashing(num_hash, all_shingle_list,result_list)
    print("Min-Hash Signature for the Documents")
    for doc_min_hash in doc_min_hash_list:
        doc,min_hash = doc_min_hash[0],doc_min_hash[1]
        print("{0}:{1}".format(doc,min_hash))
    
    displayEstimatedJaccardSimilarity(doc_min_hash_list, num_hash)
    print("")
        
    candidate_pairs = lsh(num_hash, threshold, doc_min_hash_list)
    print("Candidate pairs obtained using LSH")
    for pair in candidate_pairs:
        print("('{0}','{1}')".format(pair[0],pair[1]))
