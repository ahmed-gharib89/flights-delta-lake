#!/c/Users/aghar/anaconda3/envs/ds/python
# -*- coding: utf-8 -*-
#
# PROGRAMMER: Ahmed Gharib
# DATE CREATED: 30/06/2021
# REVISED DATE:
# PURPOSE: Helper class for analysing and visualizing spark df
#
##
# Imports python modules
import pandas as pd
import pyspark.sql.functions  as F
from pyspark.sql.dataframe import DataFrame
import matplotlib.pyplot as plt
import seaborn as sns
from functools import reduce
from itertools import combinations

# Setting up the general theme of charts and color palette to use
sns.set_theme(style='white', palette='Set2')
base_color = '#00334e'
title_font = {'fontsize': 20, 'fontweight':'bold'}
axis_font = {'fontsize': 14}


class Cache():
    def __init__(self):
        """Initialize the Cache class

        """
        self.num_rows = None
        self.nulls_df = None
        self.col_counts = {}

    def set_num_rows(self, num_rows):
        """Sets the value of num_rows attribute

        Args:
            num_rows (int): Integer value of number of rows
        """
        self.num_rows = num_rows

    def get_num_rows(self):
        """Gets the value of num_rows attribute

        Returns:
            int: Integer value of number of rows
        """
        return self.num_rows

    def set_nulls_df(self, nulls_df):
        """Sets the value of nulls_df attribute

        Args:
            nulls_df (df): pandas dataframe 
        """
        self.nulls_df = nulls_df

    def get_nulls_df(self):
        """Gets the value of nulls_df attribute

        Returns:
            df: pandas dataframe 
        """
        return self.nulls_df

    def set_col_counts(self, col_name, dataframe, top, n):
        """Sets the value of col_counts

        Args:
            col_name (str): column name
            dataframe (df): pandas dataframe to be cached
            top (bool): to determine weither it's top or bottom dataframe
            n (int): number of values in the dataframe
        """
        key = f"{col_name}_{'top' if top else 'bottom'}_{n}"
        self.col_counts[key] = dataframe

    def get_col_counts(self, col_name, top, n):
        """Gets the dataframe from the cached col_counts

        Args:
            col_name (str): column name
            top (bool): to determine weither it's top or bottom dataframe
            n (int): number of values in the dataframe
        
        Returns:
            df: pandas dataframe
        """
        key = f"{col_name}_{'top' if top else 'bottom'}_{n}"
        return self.col_counts.get(key, None)


class ExploreDfs():
    def __init__(self, dataframe, df_name):
        """Initialize ExploreDfs class and adds dataframe to its dfs attribute

        Args:
            dataframe (DataFrame): spark dataframe
            df_name (str): dataframe name 
        """
        self.dfs = {}
        self.caches = {}
        self.size = 0
        self.add(dataframe, df_name)

    def add(self, dataframe, df_name):
        """Adds dataframe to dfs attribute

        Args:
            dataframe (DataFrame): spark dataframe
        """
        # Check for the type before adding the dataframe
        if self._check_type(dataframe):
            self.dfs[df_name] = dataframe
            # Add new cache object to self.caches dictionary
            self.caches[df_name] = Cache()
            self.size += 1
        else:
            print(f"Can't add {df_name} with type {type(dataframe)}")

    def explore_nulls(self, bar_chart=True):
        """Getting the number of null values for each column in each dataframe in self.dfs
        and merge the dataframe if more than one is available

        Args:
            bar_chart (bool, optional): plot a bar chart for number of null values in each dataframe 
                                        in self.dfs if True. Defaults to True.

        Returns:
            df: pandas dataframe
        """
        # Empty list to hold null_dfs
        dfs = []
        # Loop through self.dfs items and get nulls_df 
        for df_name, dataframe in self.dfs.items():
            cache = self.caches[df_name]
            nulls_df = cache.get_nulls_df()
            if not nulls_df is None:
                dfs.append(nulls_df)
            else:
                dfs.append(self._count_nulls(dataframe, df_name))

        # If we have more than one dataframe merge them
        if self.size > 1:  
            nulls_df = reduce(lambda df1, df2: pd.merge(df1, df2, on='column_name', how='outer'), dfs)
            # Get the list of combinations of 2 columns
            cols = [col for col in nulls_df.columns if col != 'column_name']
            combs = list(combinations(cols, 2))
            # Add variance column between each combination of 2 columns
            for col1, col2 in combs:
                nulls_df[f'var_{col1}_{col2}'] = nulls_df[col1] - nulls_df[col2]
        else:
            nulls_df = dfs[0]

        # If bar_chart is True call self._bar_chart to plot the nulls_df
        if bar_chart:
            self._bar_chart(nulls_df, 'column_name', 'null values')

        return nulls_df

    def top_bottom_n(self, col_name, top=True, n=10, bar_chart=True):
        """Groupe each dataframe by col_name
        calculate the top or bottom n counts
        plot a bar chart for the result if bar_chart is True

        Args:
            col_name (str): column name to group by
            top (bool, optional): weither to calculate the top or bottom n. Defaults to True.
            n (int, optional): Number of values to return. Defaults to 10.
            bar_chart (bool, optional): Plot bar chart of the result if true. Defaults to True.

        Returns:
            df: Pandas Dataframe with the result
        """
        # Empty list to hold count_dfs  
        dfs = []
        # Loop through self.dfs items and get nulls_df 
        for df_name, dataframe in self.dfs.items():
            cache = self.caches[df_name]
            count_df = cache.get_col_counts(col_name, top, n)
            if not count_df is None:
                dfs.append(count_df)
            else:
                dfs.append(self._top_bottom_n_count(dataframe, df_name, col_name, top, n))
        
        if self.size > 1:  
            count_df = reduce(lambda df1, df2: pd.merge(df1, df2, on=col_name, how='outer'), dfs)
            # Get the list of combinations of 2 columns
            cols = [col for col in count_df.columns if col != col_name]
            combs = list(combinations(cols, 2))
            # Add variance column between each combination of 2 columns
            for col1, col2 in combs:
                count_df[f'var_{col1}_{col2}'] = count_df[col1] - count_df[col2]
        else:
            count_df = dfs[0]

        if bar_chart:
            title = f"{'Top' if top else 'Bottom'} {n} {col_name}"
            self._bar_chart(count_df, col_name, title)

        return count_df

    def _top_bottom_n_count(self, dataframe, df_name, col, top, n):
        """Helper function to calculate top or bottom n

        Args:
            dataframe (DataFrame): spark dataframe to calculate top n
            df_name (str): dataframe name
            col (str): column name to group by
            top (bool): calculate top values if True else bottom
            n (int): number of values to return

        Returns:
            df: Pandas Dataframe with the result
        """
        if not col in dataframe.columns:
            print(f'{col} is not present in {dataframe}')
            return
        else:
            dataframe = dataframe.groupby(col).count()
            if top:
                dataframe = dataframe.orderBy(F.col('count').desc())
            else:
                dataframe = dataframe.orderBy(F.col('count'))
            dataframe = dataframe.withColumnRenamed('count', df_name).limit(n).toPandas()
            cache = self.caches[df_name]
            cache = self.caches[df_name]
            num_rows = cache.get_num_rows()
            if num_rows is None:
                num_rows = self.dfs[df_name].count()
            cache.set_num_rows(num_rows)
            cache.set_col_counts(col, dataframe, top, n)
            return dataframe

    def _count_nulls(self, dataframe, df_name):
        """Count the number of nulls in each column for a given dataframe

        Args:
            dataframe (dataframe): spark dataframe
            df_name (str): dataframe name
        Returns:
            df: pandas dataframe with column names, null values and percentage of nulls
            int: number of rows in the dataframe
        """
        cols = dataframe.columns
        null_values = []
        for col in cols:
            num_nulls = dataframe.filter(dataframe[col].isNull()).count()
            null_values.append(num_nulls)
        
        nulls_dict = {
            'column_name': cols,
            df_name: null_values
        }
        nulls_df = pd.DataFrame.from_dict(nulls_dict)
        nulls_df = nulls_df.sort_values(df_name, ascending=False)
        # updating the cache for the dataframe
        cache = self.caches[df_name]
        num_rows = cache.get_num_rows()
        if num_rows is None:
            num_rows = dataframe.count()
        cache.set_num_rows(num_rows)
        cache.set_nulls_df(nulls_df)
        return nulls_df
    
    def _bar_chart(self, dataframe, col_name, title):
        """Helper function to plot bar chart for each column in the dataframe

        Args:
            dataframe (df): Pandas dataframe
            col_name (str): column name to be used as base column
        """
        # Get the list of columns to be plotted
        cols = [col for col in dataframe.columns if col != col_name and not col.startswith('var')]
        ncols = len(cols)
        fig_width = 6 * ncols
        fig_height = 8
        # Set the suplots with ncols equal to number of columns to be plotted
        fig, axis = plt.subplots(figsize=(fig_width, fig_height), ncols=ncols)
        # Adjust the space between suplots
        fig.tight_layout()
        plt.subplots_adjust(wspace = 0.5)
        # Make axis as list if only one is availble
        if ncols == 1:
            axis = [axis]
        # Loop through each column and plot
        for i, col in enumerate(cols):
            # Getting the cache
            cache = self.caches[col]
            # Get the percentage to annotate
            num_rows = cache.get_num_rows()
            percent = [f'{val / num_rows:.2%}' for val in dataframe[col]]
            ax = axis[i]
            sns.barplot(data=dataframe, y=col_name, x=col, color=base_color, ax=ax)
            sns.despine()
            # Annotate the percentage
            for i, c in enumerate(dataframe[col]):
                ax.text(c+1000, i, percent[i], va='center', size=14)
            ax.set_title(f'{col} {title}', fontdict=title_font)
            ax.set_xlabel('Count', fontdict=axis_font)
            ax.set_ylabel(f'{col_name}', fontdict=axis_font)
        plt.show()

    def _check_type(self, dataframe):
        """Checks if the dataframe type is pyspark.sql.dataframe.DataFrame

        Args:
            dataframe (Variable): the variable we need to check if its type is DataFrame

        Returns:
            bool: True if the type is DataFrame else False
        """
        return type(dataframe) == DataFrame

