import sys

#store mapping from user name to index
#e.g. {"userA":0,"userB":1,...}
user_dict = {}
#store mapping from item name to index
#e.g. {"itemA":0,"itemB":1,...}
item_dict = {}

inverse_item_dict = {}

def computeCorrelation(column_a,column_b):
    
    #pairs of co-rated items
    pairs = zip(column_a,column_b)
    pairs = filter(lambda x:x[0]!=None and x[1]!=None ,pairs)
    
    #if no co-rated items at all
    if(len(pairs)==0):
        return 0.0
    
    #compute means of both columns based on co-rated items
    a_filter = [p[0] for p in pairs]
    r_mean_a = sum(a_filter) / len(a_filter)
    b_filter = [p[1] for p in pairs]
    r_mean_b = sum(b_filter) / len(b_filter)
    
    sum_numerator = 0.0
    sum_denominator_a = 0.0
    sum_denominator_b = 0.0
    
    for pair in pairs:
        a_score,b_score = pair[0],pair[1]
        x = a_score - r_mean_a
        y = b_score - r_mean_b
        sum_numerator += (x*y)
        sum_denominator_a += x ** 2
        sum_denominator_b += y ** 2
    
    denominator = (sum_denominator_a ** 0.5) * (sum_denominator_b ** 0.5)
    
    if(denominator == 0):
        return 0.0
    
    return sum_numerator/denominator
        
def getColumnVector(m,column_index):
    """
    \n return - column column_index for matrix m
    """
    l = []
    for row in m:
        l.append(row[column_index])
    return l

def getRowVector(m,row_index):
    """
    \n return - row row_index for matrix m
    """
    return m[row_index]

def getUserIndex(username):
    return user_dict[username]

def getItemIndex(itemname):
    return item_dict[itemname]

def getScore(username,itemname,m):
    """
    \n m - utility matrix
    """
    return m[getUserIndex(username)][getItemIndex(itemname)]

def readTSV(file_name): 
    global user_dict
    global item_dict
    global inverse_item_dict
    """
    \n read .tsv, and translate users and items into index and keep them in matrix structure
    \n return - utility matrix, the un-scored items will have type None
    \n format - ItemA,ItemB,ItemC,...
    userA         x     x     x
    userB         x     x     x
    userC         x     x     x
    ...
    """   
    #initialize user_dict and item_dict
    for line in open(file_name):
        l = line.replace("\n","")
        if(len(l)==0):
            break
        fields = l.split("\t")
        
        user_name,item_name = fields[0],fields[2]
        
        if(user_name not in user_dict):
            if(len(user_dict) == 0):
                user_dict[user_name] = 0
            else:
                next_index = max(user_dict.values()) + 1
                user_dict[user_name] = next_index
                
        if(item_name not in item_dict):
            if(len(item_dict) == 0):
                item_dict[item_name] = 0
            else:
                next_index = max(item_dict.values()) + 1
                item_dict[item_name] = next_index      
                  
    #generate utility matrix  
    utility_matrix = []
    for line in open(file_name):
        l = line.replace("\n","")
        if(len(l)==0):
            break
        fields = l.split("\t")
        
        user_name,score,item_name = fields[0],float(fields[1]),fields[2]
        user_index,item_index = user_dict[user_name],item_dict[item_name]
        if(len(utility_matrix) < user_index + 1):
            utility_matrix.append([None]*len(item_dict))
        utility_matrix[user_index][item_index] = score
    
    #generate item_id <-> item_name invert index
    inverse_item_dict = {v: k for k, v in item_dict.items()}

    return utility_matrix              

def getRatedColumnIndexs(row_index,m):
    """
    \n get all column indexs of row_index that have score
    """
    scores = m[row_index]
    return [i for i in range(len(scores)) if scores[i] != None]

def computeRating(utility_matrix,user_index,item_index,rated_column_indexs,num_neighbor):
    """
    \n compute Item-Item based Collaborative Filtering rating from User u(user_index) and Item i(item_index)
    \n rated_column_indexs - column indexs to comput similarity
    \n formtat: [0,1,10,...]
    """
    #list of similarity score for (column_index,similarity)
    #format: [(0,0.5),(1,-0.5),(2,0.7),...]
    scores = []
    active_column = getColumnVector(utility_matrix, item_index)
    for rated_column_index in rated_column_indexs:
        neighbor_column = getColumnVector(utility_matrix, rated_column_index)
        weight = computeCorrelation(active_column, neighbor_column)
        scores.append((rated_column_index,weight))
    
    #generate tuple(item_name,weight)
    scores_temp = [(inverse_item_dict[x[0]],x[1]) for x in scores]
    scores = scores_temp
    #sort scores by its corresponding weight desc and name asc
    scores = sorted(scores,key=lambda x:(-x[1],x[0]))
    #pick the first num_neighbor neighbors
    scores = scores[0:min(num_neighbor,len(scores))]
    
    sum_numerator = 0.0
    sum_denominatior = 0.0
    
    for score in scores:
        column_index,weight = getItemIndex(score[0]),score[1]
        user_rate = utility_matrix[user_index][column_index]
        sum_numerator += user_rate * weight
        sum_denominatior += abs(weight)
    
    if(sum_denominatior == 0):
        return 0.0
        
    return sum_numerator / sum_denominatior      

if __name__ == '__main__':                
    tsv_file = sys.argv[1]
    username = sys.argv[2]
    num_neighbor = int(sys.argv[3])
    num_result = int(sys.argv[4])
                 
    utility_matrix = readTSV(tsv_file)        
    row_index = getUserIndex(username)
    
    #only these columns will be used to predict user's scores
    rated_column_indexs = getRatedColumnIndexs(row_index, utility_matrix) 
    
    predict_ratings = []
    user_scores = getRowVector(utility_matrix, row_index)
    for column_index,user_score in enumerate(user_scores):
        if(user_score != None):
            continue
        #predict the score for user U for the current item (row_index,column_index)
        predict_rating = computeRating(utility_matrix, row_index, column_index, rated_column_indexs, num_neighbor)
        predict_ratings.append((column_index,predict_rating))
    
    #sort predicted ratings based on ratings
    predict_ratings = sorted(predict_ratings,key=lambda x:x[1])
    predict_ratings.reverse()
    #print out the result
    num_result = min(num_result,len(predict_ratings))
    result_list = []
    for i in range(num_result):
        item_index,rating = predict_ratings[i][0],predict_ratings[i][1]
        item_name = inverse_item_dict[item_index]
        result_list.append((rating,item_name))
        
    #sort result_list by rating desc and item_name asc
    result_list = sorted(result_list,key = lambda y:(-y[0],y[1]))
    for result in result_list:
        rating,item_name = result[0],result[1]
        rating = round(rating,5)
        print("{0} {1}".format(item_name,rating))
      