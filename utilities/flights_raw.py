#!/c/Users/aghar/anaconda3/envs/ds/python
# -*- coding: utf-8 -*-
#
# PROGRAMMER: Ahmed Gharib
# DATE CREATED: 24/06/2021
# REVISED DATE:
# PURPOSE: Helper Class to retrive and convert raw data for US Immigrations dataset
#
##
# Imports python modules
import os
import pandas as pd
from glob import glob
from tqdm import tqdm
import logging
import shutil


log_file = os.path.join(os.getcwd(), "logs.log")
FORMAT = "[%(threadName)s, %(asctime)s, %(levelname)s] %(message)s"
logging.basicConfig(filename=log_file, level=logging.DEBUG, format=FORMAT)


class FlightsRaw():
    """class to prepare and retrive flights raw data
    """
    def __init__(self, source, target):
        """Initialize the ImmigrationRaw class with data source location files

        Args:
            data_dir (str): path for the data directory
        """
        # Getting the list of files paths
        self.source = glob(source + '*.csv', recursive=True)
        # Target directory
        self.target = target
        # Initialize empty list to hold retrived filesx
        self.retrived = {}
        self.rows_retrived = 0
        logging.info('Initialized FlightsRaw Class')

    def retrive(self, num_files=1, multiprocess=False):
        """Prepare the raw data by spliting the full year files into months
        and rename the columns of the files after Apr2018 to match the older files
        and move the transformed files to target folder.

        Args:
            num_files (int, optional): Number of files to transform. Defaults to 1.
            multiprocess (bool, optional): use multiprocessing to transfor files or not. Defaults to False.
        """
        temp_dir = os.path.join(os.getcwd(), 'temp')
        if not os.path.isdir(temp_dir):
            os.mkdir(temp_dir)

        for i in range(num_files):
            # Checking if the source list is not empty
            if not self.source:
                logging.warning("There is no files left to retrive")
                return
            # Getting the first file in the source list
            filepath = self.source.pop(0)
            # getting the schema and number of rows for the file
            df, meta = pyreadstat.read_sas7bdat(filepath)
            num_rows = meta.number_rows
            if self.schema == '':
                self.schema = schema = ', '.join(
                    key + ' ' + val.upper() for key, val in meta.readstat_variable_types.items())
            # Getting number of chunks for progress bar
            num_chunks = num_rows // chunksize + 1
            # Reader to read SAS file in chuncks
            reader = pyreadstat.read_file_in_chunks(pyreadstat.read_sas7bdat, filepath, chunksize=chunksize)
            # Getting the file name
            filename = filepath.split(os.sep)[-1][:-9]
            # Index number to save files in parts
            i = 1
            # Setting the progress bar
            with tqdm(total=num_chunks) as progress_bar:
                progress_bar.set_description(f'Retriving... {filename}')
                # Looping through the reader generator
                for df, meta in reader:
                    temp_name = save_name = os.path.join(temp_dir, filename + '_part_' + str(i) + '.csv')
                    save_name = os.path.join(self.target, filename + '_part_' + str(i) + '.csv')
                    if not os.path.isfile(temp_name):
                        df.to_csv(temp_name, index=False)
                    shutil.move(temp_name, save_name)
                    progress_bar.set_description(f'Retriving... {filename} part: {str(i)}')
                    progress_bar.update(1)
                    i += 1
            self.retrived[filename] = num_rows
            self.rows_retrived += num_rows
            logging.info(f'Retrived file: {filename} with total number of rows: {num_rows:,}')
        shutil.rmtree(temp_dir)

