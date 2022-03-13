import sys
import pandas as pd
from pyspark.sql.types import *

df = pd.read_csv('/dbfs/tmp/posthog.csv', sep='|',delimiter=None)


def equivalent_type(f):
  if f == 'datetime64[ns]': return DateType()
  elif f == 'int64': return LongType()
  elif f == 'int32': return IntegerType()
  elif f == 'float64': return FloatType()
  else: return StringType()

def define_structure(string, format_type):
  try: typo = equivalent_type(format_type)
  except: typo = StringType()
  return StructField(string, typo)

def pandas_to_spark(df_pandas):
  columns = list(df_pandas.columns)
  types = list(df_pandas.dtypes)
  struct_list = []
  for column, typo in zip(columns, types): 
    struct_list.append(define_structure(column, typo))
  p_schema = StructType(struct_list)
  return sqlContext.createDataFrame(df_pandas, p_schema) 


sdf = pandas_to_spark(df)
permanent_table_name = sys.argv[1]

try:
    sdf.write.saveAsTable(permanent_table_name)
except:
    sdf.write.insertInto(permanent_table_name,overwrite=False)

