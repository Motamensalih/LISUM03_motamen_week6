
import logging  
import os
import subprocess
import yaml
import pandas as pd
import gc
import datetime
import re
from os.path import getsize
import gzip

################
# FILE READING #
################

def read_config_file(filepath):
  with open(filepath , 'r') as stream:
    try:
      return yaml.safe_load(stream)
    except yaml.YAMLError as exc:
      logging.error(exc)


def replacer(string , char):
  pattern = char + '{2,}'
  string = re.sub(pattern , char , string)
  return string

# This function is for removing white spaces , special characters,
# leading and trailing underscores , and replacing double characters 
# with one character

#############################################
# VALIDATION OF DATA COLUMNS WITH YAML FILE #
#############################################

def col_header_val(df , table_config):
  df.columns = df.columns.str.lower()
  df.columns = df.columns.str.replace("[^\w]" , '_' ,regex = True)
  df.columns = list(map(lambda x: x.strip('_') , list(df.columns)))
  df.columns = list(map(lambda x: replacer(x , '_') , list(df.columns)))
  expected_col = list(map(lambda x: x.lower() , table_config['columns']))
  expected_col.sort()
  df.columns = list(map(lambda x: x.lower() , list(df.columns)))
  df = df.reindex(sorted(df.columns) , axis = 1)

  if len(df.columns)== len(expected_col) and list(expected_col)== list(df.columns):
    print("column name and column length validation passed")
    return 1
  else:
    print("column name and column length validation failed")
    mismatched_columns_file = list(set(df.columns).difference(expected_col))
    print("following file columns are not in the YAML file" , mismatched_columns_file)
    missing_YAML_file = list(set(expected_col).difference(df.columns))
    print("following YAML columns are not in the file uploaded" , missing_YAML_file)
    logging.info(f"df columns:{df.columns}")
    logging.info(f"expected columns:{expected_col}")
    return 0 

def file_summary(filepath , df):
  print("size of file is {0} GB".format(round(getsize(filepath)/(1024*1024*1024) , 2)))
  print("the csv file has {0} rows".format(df.shape[0]))
  print("the csv file has {0} columns".format(len(df.columns)))

def write_file(df , root_folder , outfile , outbound_delimiter):
  outfile_name = root_folder + outfile + '.txt'
  df.to_csv(outfile_name, header=None, index=None, sep=outbound_delimiter, mode='a')
  f_in = open(outfile_name , 'rb')
  f_out = gzip.open(f'{outfile}.txt.gz', 'wb')
  f_out.writelines(f_in)
  f_out.close()
  f_in.close()




  







