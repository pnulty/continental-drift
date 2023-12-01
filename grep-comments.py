import os
import shutil
import json
import zstandard
from datetime import datetime
import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import re
import logging.handlers

# Define the directory containing .zst files on the HDD
#hdd_directory_path = 'E:/reddit/comments/'
# Define the directory to copy and save files on the SSD
#ssd_directory_path = 'D:/HDD Stuff/python/zst/comment_csvs/'

input_path = 'data/zsts/'
output_path = 'data/csvs/'

# List all .zst files in the HDD directory [
zst_files = [f for f in os.listdir(input_path) if f.endswith('.zst')]
print(zst_files)
# Check if any of the files have already been processes, and if so, remove from the file list
existing_files = [f for f in os.listdir(output_path) if f.endswith('.csv')]
existing_files = [filename.replace('.csv', '.zst') for filename in existing_files]

# Create a set from the items in the first list for faster lookup
existing_files = set(existing_files)

# Filter the second list to remove items found in the first list
zst_files = [item for item in zst_files if item not in existing_files]

# Number of CPU cores to use (change as needed)
num_cores = cpu_count() - 2

# Search term for processing (e.g., "europe")
search_terms = "europe"
search_tokens = r'\beu\b'

# for testing
#zst_files = zst_files[6:12]
#zst_file = zst_files[0]

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

def get_time():
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    #print("Current Time =", current_time)
    return(current_time)


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
    # Define the source file path on the HDD
    hdd_file_path = os.path.join(input_path, zst_file)
    
    # Define the destination file path on the SSD (CSV format)
    ssd_file_path = os.path.join(output_path, zst_file)
    
    # Copy the .zst file from HDD to SSD
    shutil.copy(hdd_file_path, ssd_file_path)
    
    if __name__ == "__main__":
        start_time = get_time()    
        file_path = ssd_file_path
        file_size = os.stat(file_path).st_size
        file_lines = 0
        file_bytes_processed = 0
        file_lines_saved = 0
        created = None
        #field = "subreddit"
        #value = "wallstreetbets"
        bad_lines = 0
        # try:
        for line, file_bytes_processed in read_lines_zst(file_path):
            try:
                obj = json.loads(line)
                created = datetime.utcfromtimestamp(int(obj['created_utc']))
                #temp = obj[field] == value
                # Do something with the object
                #if 'body' in obj and search_term in obj['body'].lower():
                #if 'body' in obj and search_term in obj['body'].lower() and 'author' in obj ):
                #if ('body' in obj and search_term in obj['body'].lower()) and (obj['author'] != "AutoModerator"):    
                #    
                #    #df = pd.concat([df, pd.DataFrame([obj])], ignore_index=True)
                #    list_of_lines.append(obj)
                #    file_lines_saved = file_lines_saved + 1
                
                if 'body' in obj and (obj['author'] != "AutoModerator"):
                    
                    if search_terms in obj['body'].lower():
                        list_of_lines.append(obj)
                        file_lines_saved = file_lines_saved + 1                                
                    else:
                        if re.search(search_tokens, obj['body'].lower()):
                            list_of_lines.append(obj)
                            file_lines_saved = file_lines_saved + 1
            
            except (KeyError, json.JSONDecodeError) as err:
                bad_lines += 1
            file_lines += 1
            if file_lines % 250000 == 0:
                log.info(f"Date parsed: {created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} - Posts saved: {file_lines_saved:,} - Bad lines : {bad_lines:,} - Bytes processed: {file_bytes_processed:,} - Total percent of file: {(file_bytes_processed / file_size) * 100:.0f}%")
    
        # except Exception as err:
        #     log.info(err)
    
        log.info(f"Complete : {file_lines:,} : {bad_lines:,}")
        print("Start time:", start_time)
        print("End time:", get_time())
        
        #create a df from the lists
        df = pd.DataFrame(list_of_lines)
        
        #clear log handlers
        log.handlers.clear()
        
        #save the df
        #df.to_csv('in_progress.csv', index=False)
        # Save the  lines as a CSV file with the same name
        ssd_csv_file_path = os.path.join(output_path, zst_file.replace('.zst', '.csv'))
        df.to_csv(ssd_csv_file_path, index=False)
        
        #delete ZST on SSD
        os.remove(ssd_file_path)
        del(list_of_lines)
        del(df)

