import MapReduce
import sys
import re
import string


mr = MapReduce.MapReduce()
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
    text_content = re.sub("[\s]retweet[\s]{1,}"," ",text_content)
    text_content = text_content.lower()
    
    #replaced punctuation with empty string, as confirmed by TA
    #text_content = text_content.encode('utf-8').translate(string.maketrans(string.punctuation, ' '*len(string.punctuation)))
    text_content = text_content.encode('utf-8').translate(None, string.punctuation)

    temp_dict = {}
    
    for w in text_content.split():
        if w in temp_dict:
            temp_dict[w] += 1
        else:
            temp_dict[w] = 1

    for word,count in temp_dict.iteritems():
        mr.emit_intermediate(word,(current_tweet_number,count))


def reducer(key, list_of_values):
    mr.emit((key,len(list_of_values),list_of_values))


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
