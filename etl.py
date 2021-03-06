import os
import re
import csv
import glob
import json
import pandas as pd
import numpy as np

from queries import *
from cassandra.cluster import Cluster


def insert_song_info_by_session_data(session, data_filepath):
    """
        Reads new events data file and inserts to raleted table(song_info_by_session) with given session for 
        select_song_info_by_session. 
    """
    try:
        # read unified event data file and insert to song_info_by_session
        with open(data_filepath, encoding = 'utf8') as f:
            csvreader = csv.reader(f)
            next(csvreader) # skip header
            i = 1
            for line in csvreader:
                session.execute(insert_song_info_by_session, (int(line[8]), int(line[4]), line[0], float(line[5]), line[9]))
                print("inserting to song_info_by_session. row number is {}".format(i))
                i += 1
    except Exception as e:
        print("song_info_by_session Record Insert Error for {}!".format(data_filepath))
        raise e

def insert_song_info_by_user_data(session, data_filepath):
    """
        Reads new events data file and inserts to raleted table(song_info_by_user) with given session 
        for select_song_info_by_user. 
    """
    try:
        # read unified event data file and insert to song_info_by_user
        with open(data_filepath, encoding = 'utf8') as f:
            csvreader = csv.reader(f)
            next(csvreader) # skip header
            i = 1
            for line in csvreader:
                session.execute(insert_song_info_by_user, (int(line[10]), int(line[8]), int(line[4]), line[0], \
                                                   line[1], line[2], line[9]))
                print("inserting to song_info_by_user. row number is {}".format(i))
                i += 1
    except Exception as e:
        print("song_info_by_user Record Insert Error for {}!".format(data_filepath))
        raise e

def insert_user_info_by_song_data(session, data_filepath):
    """
        Reads new events data file and inserts to raleted table(user_info_by_song) with given session for select_user_info_by_song. 
    """
    try:
        # read unified event data file and insert to user_info_by_song
        with open(data_filepath, encoding = 'utf8') as f:
            csvreader = csv.reader(f)
            next(csvreader) # skip header
            i = 1
            for line in csvreader:
                session.execute(insert_user_info_by_song, (line[9], int(line[10]), line[1], line[2]))
                print("inserting to user_info_by_song. row number is {}".format(i))
                i += 1
    except Exception as e:
        print("user_info_by_song Record Insert Error for {}!".format(data_filepath))
        raise e

def pre_process_data(session, data_filepath, new_file_name):
    """
        - Finds all event data csv files paths by given filepath
        - Unify all event data and write to new event csv file which is located root directory.  
    """
    
    # Finds all event data csv files paths by given filepath
    # get all files matching extension from directory
    # Get your current folder and subfolder event data
    filepaths = os.getcwd() + data_filepath

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepaths):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
 
    # get total number of files found
    num_files = len(file_path_list)
    print('{} files found in {}'.format(num_files, data_filepath))

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = [] 

    try:
        # for every filepath in the file path list 
        for f in file_path_list:

            # reading csv file 
            with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
                # creating a csv reader object 
                csvreader = csv.reader(csvfile) 
                next(csvreader)
        
                # extracting each data row one by one and append it        
                for line in csvreader:
                    full_data_rows_list.append(line)
                
            print('{} files unified to list.'.format(num_files))
    except Exception as e:
        print("Event data unify collecting error: {}".format(e))

    try:
        # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
        # Apache Cassandra tables
        csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

        with open(new_file_name, 'w', encoding = 'utf8', newline='') as f:
            writer = csv.writer(f, dialect='myDialect')
            writer.writerow(['artist','firstName','lastName','gender','itemInSession','length',\
                             'level','location','sessionId','song','userId'])
            for row in full_data_rows_list:
                if (row[0] == ''):
                    continue
                writer.writerow((row[0], row[2], row[5], row[3], row[4], row[6], row[7], row[8], \
                                 row[12], row[13], row[16]))    
        
        # check the number of rows in your csv file
        with open(new_file_name, 'r', encoding = 'utf8') as f:
            print(sum(1 for line in f))
    except Exception as e:
        print("Event data unifying error: {}".format(e))

def main():
    """
        - Establishes connection with the sparkify database and gets
    cursor to it.  
        - Prepare unified event data
        - Trigger sequentially data insert proccesses 
        - Finally close connection
    """
    # connect to Cassandra
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
        
    try:
        # set session to sparkify keyspace
        session.set_keyspace('sparkify')
        unified_event_data_file = 'event_datafile_new.csv'

        pre_process_data(session, data_filepath='/event_data', new_file_name=unified_event_data_file)
        insert_song_info_by_session_data(session, data_filepath=unified_event_data_file)
        insert_song_info_by_user_data(session, data_filepath=unified_event_data_file)        
        insert_user_info_by_song_data(session, data_filepath=unified_event_data_file)        
    except Exception as e:
        print(e)
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
