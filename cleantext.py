#!/usr/bin/env python3

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse
import json

__author__ = ""
__email__ = ""

# Depending on your implementation,
# this data may or may not be useful.
# Many students last year found it redundant.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

# You may need to write regular expressions.
# You may need to write regular expressions.
def generate_ngrams(s, n):
    s = s.lower()

#    s = s.lower()
    all_tokens = [token for token in s.split(" ") if token != ""]

    ngrams = zip(*[all_tokens[i:] for i in range(n)])
    grams = ' '.join(["_".join(ngram) for ngram in ngrams])
    act_grams = ""
    total_grams = grams.split(" ")
    for g in total_grams:
        if '.' in g or ',' in g or ';' in g or '?' in g or '!' in g or ':' in g:
            continue
        else:
            act_grams += g + " "
    return act_grams[0:len(act_grams)-1]

def get_parsed_text(text):
    tokens = text.split(" ")
    real_tokens = []
    for g in tokens:
        if '.' in g or ',' in g or ';' in g or '?' in g or '!' in g or ':' in g:
            curr_word = ""
            for i in g:
                if '.' == i or ',' == i or ';' == i or '?' == i or '!' == i or ':' == i:
                    if len(curr_word) > 0 and not curr_word[0].isalnum():
                        curr_word = curr_word[1:]
                    if len(curr_word) > 0 and not curr_word[len(curr_word)-1].isalnum():
                        curr_word = curr_word[:len(curr_word)-1]
                    if curr_word != '':
                        real_tokens.append(curr_word)
                    real_tokens.append(i)
                    curr_word = ""
                else:
                    curr_word += i
        else:
            if len(g) > 0 and not g[0].isalnum():
                g = g[1:]
            if len(g) > 0 and not g[len(g)-1].isalnum():
                g = g[:len(g)-1]
            if g != '':
                real_tokens.append(g)
#tokenized = re.findall(r"[\w\_\w]+[\w\-\w]+[\w\/\w]+|[\w']+|[.,!?;:]", text)
    return ' '.join(real_tokens).lower()

def get_unigrams(text):
    tokens = text.split(" ")
    tokenized = []
    for i in tokens:
        if '.' == i or ',' == i or ';' == i or '?' == i or '!' == i or ':' == i:
            continue
        tokenized.append(i)
    #tokenized = re.findall(r"[\w\_\w]+[\w\-\w]+[\w\/\w]+|[\w']+", text)
    return ' '.join(tokenized).lower()

def get_bigrams(text):
    return generate_ngrams(text, 2)

def get_trigrams(text):
    return generate_ngrams(text, 3)

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """
    # YOUR CODE GOES BELOW:
    print("THIS IS THE ORIGINAL:")
    print(text)
    text = text.replace("\n", " ")
    text = text.replace("\t", " ")
    n_text=''
    i = 0
    while i < len(text):
        if text[i] == '[':
            cont = ''
            end_cont = False
            actuallyEnded = False
            for j in range(i+1, len(text)):
                if text[j] == ']':
                    end_cont = True
                if not end_cont:
                    cont += text[j]
                if text[j] == ')':
                    actuallyEnded = True
                    break
            if actuallyEnded:
                i = j
                if "/r/" in cont:
                    cont = cont.replace("/r/", "r/")
                if "/u/" in cont:
                    cont = cont.replace("/u/", "u/")
                n_text += cont
            else:
                n_text += text[i]

        elif i+6 < len(text) and text[i:i+7] == "http://":
            for j in range(i+6, len(text)):
                if text[j] == ' ':
                    n_text += ' '
                    break
            i = j
        elif i+7 < len(text) and text[i:i+8] == "https://":
            for j in range(i+7, len(text)):
                if text[j] == ' ':
                    n_text += ' '
                    break
            i = j
        elif i+3 < len(text) and text[i:i+4] == "www.":
            for j in range(i+3, len(text)):
                if text[j] == ' ':
                    n_text += ' '
                    break
            i = j
        else:
            n_text+=text[i]
        i+=1
    parsed_text = get_parsed_text(n_text)
    
    unigram = generate_ngrams(parsed_text, 1)
    bigrams = get_bigrams(parsed_text)
    trigrams = get_trigrams(parsed_text)
    return_grams = []
    for i in unigram.split(" "):
        return_grams.append(i)
    
    for i in bigrams.split(" "):
        return_grams.append(i)

    for i in trigrams.split(" "):
        return_grams.append(i)

    return return_grams


if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.
    p = "/r/fucktard            re ee e        s"
    l = sanitize(p)
    print("PARSED TEXT:" , l[0])
    print("UNIGRAM:", l[1])
    print("BIGRAM:", l[2])
    print("TRIGRAM:", l[3])

#    with open('sample.json') as infile:
#        dat = infile.readlines()
#        i = 0
#        for d in dat:
#            data = json.loads(d)
#            p = data['body']
#            l = sanitize(p)
#            print("PARSED TEXT:" , l[0])
#            print("UNIGRAM:", l[1])
#            print("BIGRAM:", l[2])
#            print("TRIGRAM:", l[3])
#            if i == 29:
#                break
#            i+=1
    # We are "requiring" your write a main function so you can
    # debug your code. It will not be graded.

