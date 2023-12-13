""" testing ways to sample from zst and store in other formats

PN November 2023
"""
import io
import json
import logging.handlers
import os
import pandas as pd
import re
import zstandard
from datetime import datetime
from tqdm import tqdm

def read_lines_zst(file_name):
    with open(file_name, 'rb') as file_handle:
        buffer = ''
        reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
        while True:
            chunk = read_and_decode(reader, 2**27, (2**29) * 2)

            if not chunk:
                break
            lines = (buffer + chunk).split("\n")

            for line in lines[:-1]:
                yield line, file_handle.tell()

            buffer = lines[-1]

        reader.close()

def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
    chunk = reader.read(chunk_size)
    bytes_read += chunk_size
    if previous_chunk is not None:
        chunk = previous_chunk + chunk
    try:
        return chunk.decode()
    except UnicodeDecodeError:
        if bytes_read > max_window_size:
            raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
        log.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
        return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

zst_path = "/media/paul/EXTERNAL_US/reddit2/reddit-academic-torrents/reddit/comments"
output_path = "/media/paul/EXTERNAL_US/sampled-comments/"

periods = {"2009-11":("2009","2010","2011"),"2012-14":("2012","2013","2014"),"2015-17":("2015","2016","2017"),"2018-20":("2018","2019","2020")}


# Search term for processing (e.g., "europe")
search_terms = "europe"
search_tokens = r'\beu\b'
monthly_sample_size = 2000
period = "2018-20"

zst_files = [f for f in os.listdir(zst_path) if f.endswith('.zst') and f[3:7] in periods[period]]

for zst_file in tqdm(zst_files):
    #start logs
    print("Processing:", zst_file)
    log = logging.getLogger("bot")
    log.setLevel(logging.DEBUG)
    log.addHandler(logging.StreamHandler())
    #initiate location for data
    list_of_lines = []
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("\nCurrent Time =", current_time)
    zst_file_path = os.path.join(zst_path, zst_file)
    start_time = datetime.now().strftime("%H:%M:%S")    
    file_path = zst_file_path
    file_size = os.stat(file_path).st_size
    file_lines = 0
    file_bytes_processed = 0
    created = None
    bad_lines = 0
    i = 0
    saved_lines = 0
    
    for line, file_bytes_processed in read_lines_zst(file_path):
        i+=1
        # trying to get every ith line to sample across the whole month.
        # will still over-represent early dates in the month, need to 
        # work with uncompressed files to do this properly.
        if i % 20 != 0:
            continue
        if saved_lines >= monthly_sample_size:
            break
        try:
            obj = json.loads(line)
            created = datetime.utcfromtimestamp(int(obj['created_utc']))
            if 'body' in obj and (obj['author'] != "AutoModerator"):
                if search_terms in obj['body'].lower():
                    list_of_lines.append(obj)
                    saved_lines += 1                                
                else:
                    if re.search(search_tokens, obj['body'].lower()):
                        list_of_lines.append(obj)
                        saved_lines += 1
        
        except (KeyError, json.JSONDecodeError) as err:
            bad_lines += 1
        file_lines += 1
        if file_lines % 250000 == 0:
            log.info(f"Date parsed: {created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} - Posts saved: {saved_lines:,} - Bad lines : {bad_lines:,} - Bytes processed: {file_bytes_processed:,} - Total percent of file: {(file_bytes_processed / file_size) * 100:.0f}%")

    # except Exception as err:
    #     log.info(err)

    log.info(f"Complete : {file_lines:,} : {bad_lines:,}")
    print("Start time:", start_time)
    print("End time:", datetime.now().strftime("%H:%M:%S"))
    
    #create a df from the lists
    df = pd.DataFrame(list_of_lines)
    
    #clear log handlers
    log.handlers.clear()
    
    #save the df
    #df.to_csv('in_progress.csv', index=False)
    # Save the  lines as a CSV file with the same name
    print("Saving:", zst_file)
    ssd_csv_file_path = os.path.join(output_path, period, zst_file.replace('.zst', '.csv'))
    df.to_csv(ssd_csv_file_path, index=False)
        