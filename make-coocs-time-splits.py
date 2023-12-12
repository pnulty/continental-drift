import collections
import fastparquet
import nltk
import pandas as pd
import os
import xml.etree.ElementTree as ET
from pyarrow import feather
from tqdm import tqdm



def count_coocs_texts(doc_list, vocab):
    """
    counts word co-occurrences in a list of text segments by updating a dict
    of collections.Counters
    """
    # a dictionary of counters: coocs[word1][word2]:count
    coocs = collections.defaultdict(collections.Counter)
    for d in doc_list:
        tokens = [
            w
            for w in nltk.regexp_tokenize(
                d, pattern=r"\s+", gaps=True, discard_empty=True
            )
            if w in vocab
        ]

        for i in range(0, len(tokens)):
            coocs[tokens[i]].update(tokens[:i])
            coocs[tokens[i]].update(tokens[i:])
    return coocs


def tokenize_text(text, stopwords=frozenset(), punct="", min_chars=4):
    """
    Tokenizes a text into a list of words. Filters out stopwords and punctuation.
    """
    words = []
    for w in nltk.regexp_tokenize(text, pattern="\s+", gaps=True, discard_empty=True):
        # words shorter than three letters are omitted, words of exactly three letters are
        # only included if they are in the valid_words set
        # all words longer than three letters are included
        if (
            w not in stopwords
            and w.isalpha()
            and w not in punct
            and len(w) >= min_chars
        ):
            words.append(w)
    return words

def preview_coocs(coocs, term="democracy"):
    """
    Prints a preview of the co-occurrences in a given dictionary of counters.
    """
    for y in coocs:
        print(y)
        print(term)
        print(coocs[y][term].most_common(20))


#
# SETUP
#
lang = "fr"
stopwords = frozenset(nltk.corpus.stopwords.words("english"))
punct = "'.,;?!\""
min_freq = 20
vocab = collections.Counter()
max_iterations = 100
periods = {"2009-11":("2009","2010","2011"),"2012-14":("2012","2013","2014"),"2015-17":("2015","2016","2017"),"2018-20":("2018","2019","2020")}

for p in periods:
    directory_path = os.path.join("csvs",p)
    file_count = sum(len(files) for _, _, files in os.walk(directory_path))
    #
    # Build vocabulary
    #
    print("making vocab")
    docs = []
    i = 0
    files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
    with tqdm(total=file_count) as pbar:
        for file in files:
            pbar.update(1)
            print(file)
            df = pd.read_csv(os.path.join("csvs",p,file), encoding="utf-8", encoding_errors="replace")
            df['body'] = df['body'].astype(str).str.lower()
            tokens = df["body"].apply(lambda x: tokenize_text(x, stopwords, punct=punct))
            for t in tokens:
                vocab.update(t)

# remove words that occur less than min_freq times
cut_vocab = collections.Counter({k: c for k, c in vocab.items() if c >= min_freq})

print(cut_vocab.most_common(50))

with open("totals-all-periods.txt", "w", encoding="utf-8") as out:
    for x in cut_vocab:
        out.write(x + " \t " + str(cut_vocab[x]) + "\n")


#
# Count co-occurrences
#
print("counting co-occurrences")
min_cooc = 5

for p in periods:
    directory_path = os.path.join("csvs",p)
    file_count = sum(len(files) for _, _, files in os.walk(directory_path))
    i = 0
    coocs = collections.defaultdict(lambda: collections.defaultdict(collections.Counter))
    files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
    with tqdm(total=file_count) as pbar:
        for file in files:
            pbar.update(1)
            print(file)
            df = pd.read_csv(os.path.join("csvs",p,file))
            df['body'] = df['body'].astype(str).str.lower()
            tmp = count_coocs_texts(df['body'].tolist(), cut_vocab)
            for word1 in tmp:
                    coocs[p][word1].update(tmp[word1])


preview_coocs(coocs)
print("building dataframe")
rows_list = []
for year in coocs:
    for w1 in coocs[year]:
        for w2 in coocs[year][w1]:
            count = coocs[year][w1][w2]
            if count > min_cooc:
                rows_list.append(
                    {
                        "year": year,
                        "focal": w1,
                        "bound": w2,
                        "count": coocs[year][w1][w2],
                    }
                )

print("writing to parquet")
fastparquet.write("continent-coocs-allperiods.parquet", pd.DataFrame(rows_list))
#feather.write_feather(tmp, "un-coocs_all_yearhome_600_30.feather", version=1)
# feather.write_feather(tmp, '/home/paul/Dropbox/newest2023/networks/viewer/un-coocs_all_year.feather', version=1)