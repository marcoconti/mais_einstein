import os
import sys
import xarray as xr
from pyspark.sql import SparkSession

def get_spark_session(app_name="NotebookSession", driver_mem="8g", exec_mem="8g") -> SparkSession:
    """
    Cria e retorna uma SparkSession configurada para ambiente local.
    """
    # 1. Remove qualquer barreira de proxy local
    for env_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
        os.environ.pop(env_var, None)

    # 2. Garante que o Spark use o Python do venv ativo
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # 3. Força o IP local
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

    # 4. Inicializa o Spark
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.network.auth.enabled", "false") \
        .config("spark.driver.memory", driver_mem) \
        .config("spark.executor.memory", exec_mem) \
        .getOrCreate()
   

    return spark

def write_data_csv(df_write, write_path, file_name):
    
    df_write.toPandas().to_csv(f"{write_path}\{file_name}", index=False)


def convert_nc_to_spark_dataframe(spark, path, file_name):
    with xr.open_dataset(f"{path}\{file_name}"
                        ,engine="netcdf4"
                        ,chunks={"time": 365
                                ,"latitude": 100
                                ,"longitude": 100 }
                        ) as ds:
        
        # Transforma o Dataset em um Spark Dataframe
        df_dask     = ds.to_dask_dataframe()
        df_dask_c   = df_dask.compute()
        df_spark    = spark.createDataFrame(df_dask_c)

    return df_spark
