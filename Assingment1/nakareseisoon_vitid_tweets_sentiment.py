import sys
import string
import re
import MapReduce

mr = MapReduce.MapReduce()
scores_word = {}
scores_phrase = {}
current_tweet_number = 0


def mapper(each_tweet):
    global current_tweet_number
    
    current_tweet_number += 1
    text_content = each_tweet["text"]
    text_content = text_content.replace("\t"," ")
    text_content = re.sub("\s{2,}"," ",text_content)
    text_content = " " + text_content + " "
    text_content = re.sub("http[s]?://[^\s]*"," ",text_content)
    text_content = re.sub("[\s]RT[\s]{1,}@[^:]*:"," ",text_content)    
    text_content = re.sub("[#@][^\s]{1,}"," ",text_content)
    text_content = text_content.lower()
    
    tweet_score = 0.0
    
    #handle phrases first
    for phrase,score in scores_phrase.iteritems():
        occurence = len(re.findall("(?=([^a-zA-Z0-9]" + phrase + "[^a-zA-Z0-9]))",text_content))
        if occurence > 0: 
            tweet_score += occurence*score
            text_content = re.sub("[^a-zA-Z0-9]" + phrase + "(?=[^a-zA-Z0-9])"," ",text_content)
    
    #replaced punctuation with empty string, as confirmed by TA
    #text_content = text_content.encode('utf-8').translate(string.maketrans(string.punctuation, ' '*len(string.punctuation)))
    text_content = text_content.encode('utf-8').translate(None, string.punctuation)
    
    #then handle word
    words = text_content.split()    
    for w in words:
        if w in scores_word:
            tweet_score += scores_word[w]
            
    mr.emit_intermediate(current_tweet_number, tweet_score)
  
def reducer(key, list_of_values):
    # Reducer code goes in here
    mr.emit((key, list_of_values[0]))

if __name__ == '__main__':
    afinnfile = open(sys.argv[1])       # Make dictionary out of AFINN_111.txt file.
    for line in afinnfile:
        term, score  = line.split("\t")  # The file is tab-delimited. #\t means the tab character.
        if(len(term.split())>1):
            scores_phrase[term] = int(score)
        else:
            scores_word[term] = int(score)  # Convert the score to an integer.
    tweet_data = open(sys.argv[2])
    mr.execute(tweet_data, mapper, reducer)


