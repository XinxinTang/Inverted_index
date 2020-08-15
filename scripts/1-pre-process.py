#!/usr/bin/env python
# coding: utf-8

# In[1]:


# !pip list


# In[2]:


import pandas as pd 
import re
from glob import glob


# In[3]:


# Build a dictionary
word_set = set()
for filename in glob("./dataset/*"):
#     print(filename)
    file_id = filename.split("/")[-1]
    with open(filename, "rb") as f:
        lines = f.readlines()
        for line in lines:
            line = line.replace(b"\n", b"").strip()
            if len(line) > 0:
                for word in line.split(b" "):
                    word = re.sub(b"[\W+]+", b"", word)
                    word_set.add(word)

dic = {"words": list(word_set)}
df_dic = pd.DataFrame.from_dict(dic)
df_dic.sort_values(by=["words"], inplace=True) 
df_dic.reset_index(drop=True, inplace=True)
df_dic['wordId'] = df_dic.index
print(df_dic.head())
# df.to_csv("./dictionary.csv", index=True)


# In[4]:


# generate pair (wordId, docId) 
df_all = pd.DataFrame()
for filename in glob("./dataset/*"):
    file_id = filename.split("/")[-1]
    wordSet_one_file = set()
    with open(filename, "rb") as f:
        lines = f.readlines()
        for line in lines:
            line = line.replace(b"\n", b"").strip()
            if len(line) > 0:
                for word in line.split(b" "):
                    word = re.sub(b"[\W+]+", b"", word)
                    wordSet_one_file.add(word)
        dic_one_file = {"words": list(wordSet_one_file)}
        df = pd.DataFrame.from_dict(dic_one_file)
        df["docId"] = file_id
        if df_all.empty:
            df_all = df
        else:
            df_all = df_all.append(df)
print("Done! pair (wordId, docId) ")

# generate pair (wordId, docId_list)
df_join = df_all.merge(df_dic, on="words")
print("Done! Join two DF")
df_join.drop(columns=["words"], inplace=True)
df_join.to_csv("pair_wordId_docId.csv", index=False)

# Inverted index 
# df_join == df_join.groupby(["wordId"])["docId"].apply(list)
# df_join.head(20)
# 
                    
                    






