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
import psutil
from multiprocessing import current_process, Pool, cpu_count
from sys import stdout
import pickle


log_file = os.path.join(os.getcwd(), "logs.log")
FORMAT = "[%(threadName)s, %(asctime)s, %(levelname)s] %(message)s"
logging.basicConfig(filename=log_file, level=logging.DEBUG, format=FORMAT)


class FlightsRaw():
    """class to prepare and retrive flights raw data
    """
    def __init__(self, checkpoint, source=None, target=None):
        """Initialize the FlightsClass class with data source location files
        and load the last status if checkpoint is provided

        Args:
            checkpoint (str): path for checkpoint directory.
            source (str, optional): path for target folder required if no checkpoint provided. Defaults to None.
            target (str, optional): path for the data directory required if no checkpoint provided. Defaults to None.
        """
        # Getting the list of files paths if source is provided
        if source:
            self.source = glob(source + '**' + os.sep + '*.csv', recursive=True)
        else:
            self.source = source
        # Target directory
        self.target = target
        # Initialize empty dictionary to hold retrived files and number of rows for each file
        self.retrived = {}
        self.rows_retrived = 0
        # Empty list to hold the path for files retrived
        self.retrived_files = []
        self.checkpoint = checkpoint

        if not self.source:
            print('No source is provided trying to load from latest checkpoint')
            self.load()

        logging.info('Initialized FlightsRaw Class')

    def retrive(self, num_files=1, multiprocess=False):
        """Prepare the raw data by spliting the full year files into months
        and rename the columns of the files after Apr2018 to match the older files
        and move the transformed files to target folder.

        Args:
            num_files (int, optional): Number of files to transform. Defaults to 1.
            multiprocess (bool, optional): use multiprocessing to transfor files or not. Defaults to False.
        """
        # temp folder to hold the transformed csv file until they saved completly 
        # and the we move them to target folder to avoid spark stream from transforming incomplete files
        temp_folder = os.path.join(os.getcwd(), '.temp')
        if not os.path.isdir(temp_folder):
            os.mkdir(temp_folder)

        # Check if number of files left less than what asked to retrive
        if len(self.source) < num_files:
            num_files = len(self.source)
            print(f"Only {num_files} files left to retrive")

        # List of files to process
        files = [self.source[i] for i in range(num_files)]
        # if multiprocess is set to True
        if multiprocess:
            # Setting the arguments list of tuples for processing pool
            d_args = [(file, temp_folder) for file in files]
            # Set number of threads to the number of cpu cores
            num_workers = cpu_count()
            # Initialize the pool with num workers
            pool = Pool(processes=num_workers)
            # Getting the output for updating the class attributes
            output = pool.starmap(self._process_file, d_args)
            pool.close()
            pool.join()
            # Updating the class attributes
            for retrived_file, num_rows_retrived, retrived_paths in output:
                self.retrived[self._get_file_name(retrived_file)] = num_rows_retrived
                self.rows_retrived += num_rows_retrived
                self.source.remove(retrived_file)
                for retrived_path in retrived_paths:
                    self.retrived_files.append(retrived_path)
        else:
            # Normal looping through file by file
            for file in files:
                retrived_file, num_rows_retrived, retrived_paths = self._process_file(file, temp_folder)
                self.retrived[self._get_file_name(retrived_file)] = num_rows_retrived
                self.rows_retrived += num_rows_retrived
                self.source.remove(retrived_file)
                for retrived_path in retrived_paths:
                    self.retrived_files.append(retrived_path)

        shutil.rmtree(temp_folder)


    def _process_file(self, filepath, temp_folder):
        """Helper function to actually process file by file we call it from retrive eaither by multiprocessing or normal

        Args:
            filepath (str): path for the file to process
            temp_folder (str): path for the temp directory to stage the file before moving to target folder

        Returns:
            tuple: tuple of (filepath, num_rows, retrived_files) to update the class attributes
        """
        
        columns = ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'CRSDepTime',
                   'ArrTime', 'CRSArrTime', 'UniqueCarrier', 'FlightNum', 'TailNum',
                   'ActualElapsedTime', 'CRSElapsedTime', 'AirTime', 'ArrDelay',
                   'DepDelay', 'Origin', 'Dest', 'Distance', 'TaxiIn', 'TaxiOut',
                   'Cancelled', 'CancellationCode', 'Diverted', 'CarrierDelay',
                   'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay']

        rename_dict = {
            'YEAR': 'Year',
            'MONTH': 'Month',
            'DAY_OF_MONTH': 'DayofMonth',
            'DAY_OF_WEEK': 'DayOfWeek',
            'DEP_TIME': 'DepTime',
            'CRS_DEP_TIME': 'CRSDepTime',
            'ARR_TIME': 'ArrTime',
            'CRS_ARR_TIME': 'CRSArrTime',
            'OP_UNIQUE_CARRIER': 'UniqueCarrier',
            'OP_CARRIER_FL_NUM': 'FlightNum',
            'TAIL_NUM': 'TailNum',
            'ACTUAL_ELAPSED_TIME': 'ActualElapsedTime',
            'CRS_ELAPSED_TIME': 'CRSElapsedTime',
            'AIR_TIME': 'AirTime',
            'ARR_DELAY': 'ArrDelay',
            'DEP_DELAY': 'DepDelay',
            'ORIGIN': 'Origin',
            'DEST': 'Dest',
            'DISTANCE': 'Distance',
            'TAXI_IN': 'TaxiIn',
            'TAXI_OUT': 'TaxiOut',
            'CANCELLED': 'Cancelled',
            'CANCELLATION_CODE': 'CancellationCode',
            'DIVERTED': 'Diverted',
            'CARRIER_DELAY': 'CarrierDelay',
            'WEATHER_DELAY': 'WeatherDelay',
            'NAS_DELAY': 'NASDelay',
            'SECURITY_DELAY': 'SecurityDelay',
            'LATE_AIRCRAFT_DELAY': 'LateAircraftDelay'
        }

        # Reading the file as pandas dataframe
        df = pd.read_csv(filepath)
        # Getting the columns to check if we need to rename them
        df_cols = list(df.columns)
        if df_cols != columns:
            df.rename(columns=rename_dict, inplace=True)
        
        # Making sure that we get the dataframe with the same column sort
        df = df[columns]

        # Getting number of rows
        num_rows = df.shape[0]
        # List of months in each file
        months = df.Month.unique().tolist()
        # Getting the year
        year = df.iloc[0, 0]
        # Setting the target directory to save the files
        target_dir = self.target + str(year)
        if not os.path.isdir(target_dir):
            os.mkdir(target_dir)
        filename = self._get_file_name(filepath)
        retrived_files = []
        for month in months:
            # Writing to the terminal instead of jupyter to remove noise
            stdout.write(f'Retriving... month: {str(month)} from file: {filename} ')
            stdout.flush()
            # FIltering by month
            temp_df = df[df.Month == month]
            save_path = target_dir + os.sep + str(year * 100 + month) + '.csv'
            temp_path = temp_folder + os.sep + str(year * 100 + month) + '.csv'
            # Save the dataframe to csv to temp folder then move it to target folder
            temp_df.to_csv(temp_path, index=False)
            shutil.move(temp_path, save_path)
            retrived_files.append(save_path)
        logging.info(f'Retrived file: {filename} with total number of rows: {num_rows:,}')
        return filepath, num_rows, retrived_files
        
    def _get_file_name(self, file_path):
        """Helper function to get the file name from file path

        Args:
            file_path (str): file path

        Returns:
            str: filename
        """
        filename = file_path.split(os.sep)[-1][:-4]
        return filename

    def save(self):
        """Get the current state of the class and save it to pickle file
        """
        state_dict = {
            'source': self.source,
            'target': self.target,
            'retrived': self.retrived,
            'rows_retrived': self.rows_retrived,
            'retrived_files': self.retrived_files
        } 
        # Check if checkpoint directory not exists and create it
        if not os.path.isdir(self.checkpoint):
            os.mkdir(self.checkpoint)

        # Save the state_dict as pickle file
        with open(self.checkpoint + 'flight_raw.pickle', "wb") as pickle_file:
            pickle.dump(state_dict, pickle_file)

        print(f"Saved checkpoint at {self.checkpoint + 'flight_raw.pickle'}")

    def load(self):
        """Loads state_dict from pickle file and set the class attributes
        """
        # Check first if there is a checkpoint available
        if not os.path.isfile(self.checkpoint + 'flight_raw.pickle'):
            print(f'No checkpoint found at {self.checkpoint}...')
            logging.warning(f'No checkpoint found at {self.checkpoint}...')
            return

        with open(self.checkpoint + 'flight_raw.pickle', 'rb') as pickle_file:
            state_dict = pickle.load(pickle_file)

        self.source = state_dict['source']
        self.target = state_dict['target']
        self.retrived = state_dict['retrived']
        self.rows_retrived = state_dict['rows_retrived']
        self.retrived_files = state_dict['retrived_files']

        print(f"loaded checkpoint from {self.checkpoint + 'flight_raw.pickle'}")
