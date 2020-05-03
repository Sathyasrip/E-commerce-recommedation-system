"""
To read the compressed Amazon products dataset and create reviews.csv file
"""

import gzip
import os.path
import pandas as pd

true = True
false = False


def parse(path):
    record_address = gzip.open(path, 'rb')

    for record in record_address:
        yield eval(record)


def openFile(path):
    """
    Unzip's the file and read into if reviews.csv doesn't exist
    write to it so it will be faster to read in next time.
       
    :param: path (str) : path of .gz jason file
    :returns: DataFrame of file contents
    :rvalue: Pandas DataFrame
    """
    if os.path.isfile('reviews.csv') is False:
        index = 0
        df = {}
        for data_row in parse(path):
            df[index] = data_row
            index += 1

        df = pd.DataFrame.from_dict(df, orient='index')
        df.to_csv('reviews.csv')
    else:
        df = pd.read_csv('reviews.csv')

    return df
