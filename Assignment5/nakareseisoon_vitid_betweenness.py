import sys

#mapping between node name and index used in adjacency_matrix
#format: {"1":0,"2":1,...}
node_dict = {}
inverse_node_dict = {}
adjacency_matrix = []
#keep 2 nodes that have edge for printing later
pairs = []

def readNodes(file_name):
    global node_dict
    global inverse_node_dict
    global adjacency_matrix
    
    for line in open(file_name):
        l = line.replace("\n","")
        if(len(l)==0):
            break
        x,y = l.split(" ")
        
        if(len(node_dict)==0):
            node_dict[x] = 0
        
        if(x not in node_dict):
            node_dict[x] = max(node_dict.values()) + 1
        if(y not in node_dict):
            node_dict[y] = max(node_dict.values()) + 1
        pairs.append([ min( int(x),int(y) ),max( int(x),int(y) )  ] )    
            
    #construct inverse dictionary also
    inverse_node_dict = {v: k for k, v in node_dict.items()}        
    #construct adjacency_matrix
    number_nodes = len(node_dict)
    for i in range(number_nodes):
        adjacency_matrix.append([0] * number_nodes)    
        
    for line in open(file_name):
        l = line.replace("\n","")
        if(len(l)==0):
            break
        x,y = l.split(" ")
        index_x = node_dict[x]
        index_y = node_dict[y]
        adjacency_matrix[index_x][index_y] = 1   
        adjacency_matrix[index_y][index_x] = 1  
        
def constructTree(root):
    #contains all nodes in upper levels
    #format {1:Node(1),2:Node(2),...}
    all_upper_nodes = {}
    #contains all nodes of the current level
    #format {1:Node(1),2:Node(2),...}
    current_level_nodes = {}
    #contains all nodes of the lower 1 level
    #format {1:Node(1),2:Node(2),...}
    current_child_nodes = {}
    
    rootNode = Node(node_name=root,sum_parent_count=0.0,num_parent=0)
    current_level_nodes[rootNode.node_name] = rootNode
    
    while(len(current_level_nodes)>0):
        for nodeObject in current_level_nodes.values():
            index = nodeObject.node_name
            rows = adjacency_matrix[index]
            #look for all nodes that this node connected to
            for connected_index,is_edge_exist in enumerate(rows):
                #no edge between node{index} and node{connected_index}
                if(is_edge_exist==0):
                    continue
                #don't perform bfs at the parents
                if(connected_index in all_upper_nodes):
                    continue
                #don't perform bfs on a node with the same level
                if(connected_index in current_level_nodes):
                    continue
                #if a child node is already connected by some nodes before
                if(connected_index in current_child_nodes):
                    childNode = current_child_nodes[connected_index]
                    childNode.increaseParentCount()
                    childNode.sum_parent_count = childNode.sum_parent_count + nodeObject.num_parent
                    nodeObject.addChild(childNode)
                    continue
                #create a new child node
                childNode = Node(connected_index,sum_parent_count=max(1.0,nodeObject.num_parent))
                nodeObject.addChild(childNode)
                #add a new chld node to current_child_nodes
                current_child_nodes[childNode.node_name] = childNode
        #add nodes in the current level into all_upper_nodes
        all_upper_nodes.update(current_level_nodes)
        #iterate overs nodes in the next lower level        
        current_level_nodes = current_child_nodes
        current_child_nodes = {}
    
    return all_upper_nodes  
            
class Node:
    #node_score = 0.0
    #edges = [(childnodeA,edgeScore_to_A),(childnodeB,edgeScore_to_B),...]
    def __init__(self,node_name,sum_parent_count,num_parent=1):  
        self.node_name = node_name
        self.node_score = 0.0
        self.edges = []
        self.num_parent = num_parent
        self.sum_parent_count = sum_parent_count
    
    def addChild(self, child_node, edge_score = 0.0):
        self.edges.append((child_node,edge_score)) 
        
    def increaseParentCount(self):
        self.num_parent += 1   
        
    def __str__(self):
        original_name = inverse_node_dict[self.node_name]
        list_child_edges = []
        for edge in self.edges:
            childNode,edge_score = edge[0],edge[1]
            original_child_node_name = inverse_node_dict[childNode.node_name]
            list_child_edges.append((original_child_node_name,edge_score))
        return "nodeName:{0},childEdges({1}),num_parent:{2},nodeScore:{3}".format(original_name,list_child_edges,self.num_parent,self.node_score)
    
    def computeScore(self):
        #if this is a leaf node
        if(len(self.edges)==0):
            self.node_score = 1.0
            return self.node_score
        
        #adjust the current score to its number of parents otherwise
        self.node_score = max(1.0,self.num_parent)
        
        accum_score = 0.0
        #update score of edges connected to all of its leaf nodes
        new_edges = []
        for child_edge in self.edges:
            childNode = child_edge[0]
            child_score = childNode.computeScore()
            edge_score = self.node_score / childNode.sum_parent_count
            edge_score = edge_score * child_score
            new_edges.append((childNode,edge_score))
            accum_score += edge_score
        self.edges = new_edges
        #update score of itself
        self.node_score = accum_score + 1.0
        return self.node_score
        
if __name__ == '__main__':     
    file_name = sys.argv[1]
    readNodes(file_name)
    #construct edge-score matrix
    edge_score_matrix = []
    for i in range(len(adjacency_matrix)):
        edge_score_matrix.append([0.0] * len(adjacency_matrix))   
    
    #loop through every nodes and add up score for each edge
    for i in range(len(node_dict)):        
        root_index = i
        result = constructTree(root_index)
        rootNode = result[root_index]
        rootNode.computeScore()
        for i_index,nodeObject in result.iteritems():
            for edge in nodeObject.edges:
                childNode,edge_score = edge[0],edge[1]
                j_index = childNode.node_name
                edge_score_matrix[i_index][j_index] = edge_score_matrix[i_index][j_index] + edge_score
                edge_score_matrix[j_index][i_index] = edge_score_matrix[i_index][j_index]
    
    #divide every scores by 2
    for i in range(len(edge_score_matrix)):
        for j in range(len(edge_score_matrix)):
            edge_score_matrix[i][j] = edge_score_matrix[i][j] / 2.0
            
    #print out the result         
    pairs = sorted(pairs)  
    for pair in pairs:
        i_index = node_dict[str(pair[0])]
        j_index = node_dict[str(pair[1])]
        print("{0} {1}".format(pair,edge_score_matrix[i_index][j_index]))        
        