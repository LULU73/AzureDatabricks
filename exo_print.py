import pandas as pd
import numpy as np

def print_hw():
    dataframe = pd.read_csv('name_list.csv') 
    for i in np.arange(len(dataframe)):
        print('Hello world !', dataframe.iloc[i,:].values[0] )

