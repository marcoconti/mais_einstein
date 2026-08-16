"""
Métodos e funções utilizados para recuperar e converter arquivos recebidos do portal Copernicus
Climate Data Store

https://cds.climate.copernicus.eu/


Lib's utilizadas:
    cdsapi -  versão 0.7.7
        Uso: Pesquisa dados do portal Climate Data Store
        Criador e Mantenedor: O ECMWF (European Centre for Medium-Range Weather Forecasts).
        É um projeto open-source (Licença Apache-2.0) mantido no GitHub oficial do ECMWF (ecmwf/cdsapi).  
        https://github.com/ecmwf/cdsapi

    xarray - versão 2026.7.0
        Uso: Tratamento de dados tabulares bidimensionais
        Criadores: Desenvolvida originalmente em 2014 por pesquisadores do NCAR (National Center for Atmospheric Research)
        Mantenedores: É um projeto open-source com apoio da fundação NumFOCUS (https://numfocus.org/)
        É um projeto open-source (Licença Apache-2.0) mantido no GitHub:
        https://docs.xarray.dev/en/stable/ - Documentação oficial
        https://github.com/xarray-contrib 

    netCDF4 - versão 1.7.4
        Uso: Descompactar arquivo .nc (netCDF4)
        Criador: Desenvolvida originalmente por Jeff Whitaker (físico e pesquisador do NOAA Physical Sciences Laboratory nos EUA).
        Mantenedores: É mantida sob o guarda-chuva da Unidata — uma organização do consórcio UCAR (University Corporation for Atmospheric Research) nos EUA, que é a própria criadora e especificadora do formato NetCDF no mundo.
        Código: Código aberto (Licença MIT) hospedado no GitHub em 
        https://github.com/Unidata/netcdf4-python

    PyArrow - versão 25.0.1
        Uso: Evita o uso excessivo de memória RAM
        Criadores: Desenvolvida em 2016 por Wes McKinney (o próprio criador da biblioteca Pandas) 
        Mantenedores: É mantida pela Apache Software Foundation como parte do projeto top-level Apache Arrow
        Código: Código aberto (Licença Apache-2.0) hospedado no repositório principal do projeto no GitHub em
        https://github.com/apache/arrow (no diretório python/pyarrow)

"""

import os, sys
import cdsapi
import xarray as xr
from datetime import datetime, timedelta

from pyspark.sql import SparkSession # Só é necessária para processamento LOCAL

RETANGULO_GEO_BRASIL = [6, -74, -34, -31] # Norte, Oeste, Sul, Leste - Retangulo geográfico, em graus, onde está o Brasil



# Deverá ser recriada para usar um cofre de senhas ou uma secret
def get_cdsapi_authentication():
    """
    Informações de autenticação na API
    *** Criar um novo contrato de autenticação deverá ser criado usando um usuário de serviços do Einstein
    C:/Users/DRT90628/.ecmwfdatastoresrc
    """

    url = os.getenv("ECMWF_DATASTORES_URL")
    key = os.getenv("ECMWF_DATASTORES_KEY")
    return url, key

def criar_client_cds_ERA5():

    # Recupera informações de autenticação no portal da Copernicus:
    url, key = get_cdsapi_authentication()

    # Cria uma instancia do cliente do CDS
    client = \
        cdsapi.Client(url = url
                     ,key = key
        )

    return client

def criar_client_cds_EAC4():
    client = cdsapi.Client(
        url = "https://ads.atmosphere.copernicus.eu/api",
        key = ""
    )

    return client

def obter_mes_dia(ano: int):
    """Retorna listas de meses e dias válidos para um ano de referência.

    A função usa a data atual para determinar o período de referência. Se
    hoje for dia 1 ou 2, considera-se o mês anterior como referência para
    evitar datasets atrasados no início do mês.

    Parâmetros:
        ano (int): Ano para o qual se deseja obter os meses e dias válidos.

    Retorna:
        tuple[list[str], list[str]]: Uma tupla contendo duas listas de
        strings formatadas com dois dígitos:
            - meses: lista de meses válidos no ano.
            - dias: lista de dias válidos para o mês de referência.

    Regras:
        - Se o parametro ano for anterior ao ano de referência, retorna todos os
          meses de 01 a 12 e todos os dias de 01 a 31.

        - Se o ano for igual ao ano de referência, retorna meses até o mês
          atual de referência e dias até D-2 para o mês atual.

        - Não permitir ano for futuro
    """

    hoje = datetime.now()

    # Se dia 01 ou 02, considerar mês anterior
    if hoje.day in (1, 2):
        data_referencia = hoje.replace(day=1) - timedelta(days=1)
    else:
        data_referencia = hoje

    ano_ref = data_referencia.year
    mes_ref = data_referencia.month

    # Lista de meses
    if ano < ano_ref:
        meses = [f"{m:02d}" for m in range(1, 13)]
        dias  = [f"{d:02d}" for d in range(1, 32)]

    elif ano == ano_ref:
        meses = [f"{m:02d}" for m in range(1, mes_ref + 1)]

        # Último dia válido considerando D-2, pois alguns dataset tem um atraso de até 2 dias
        data_limite = hoje - timedelta(days=2)

        # Se estiver no mês da data limite, retorna apenas até D-2
        dias = [f"{d:02d}" for d in range(1, data_limite.day + 1)]

    else:
        raise ValueError(f"Ano futuro não permitido: {ano}")

    return meses, dias

def recuperar_dados_ERA5(dataset_, product_type, variable, ano):
    """Faz download de um arquivo .nc com dados ERA5 do CDS e salva 'localmente' em formato NetCDF.

    Monta uma requisição para a API CDS (Copernicus Data Store) com os
    parâmetros de dataset, variável, ano, mês, dia e área geográfica, faz o
    download do arquivo NetCDF e renomeia o arquivo para a pasta local de
    processamento.

    Parâmetros:
        dataset_ (str)    : Nome do dataset ERA5 a ser consultado.
        product_type (str): Tipo de produto, por exemplo "reanalysis".
        variable (str)    : Variável meteorológica a ser baixada.
        ano (int)         : Ano do dado.

    Retorna:
        str: Caminho do arquivo baixado temporariamente pelo cliente CDS.

    Observações:
        - A função exige que as variáveis de ambiente
          ECMWF_DATASTORES_URL e ECMWF_DATASTORES_KEY estejam definidas.

          Essas variáveis deverão ser criada no ambiente onde ocorrer a execução, 
          por exemplo no Databricks sugiro a criação em uma secret

    """

    mes, dia = obter_mes_dia(ano)

    # dataset = "reanalysis-era5-single-levels"
    dataset = dataset_
    request = {
        "product_type":     [product_type],
        "variable":         [variable],
        "year":             ano,
        "month":            mes,
        "day":              dia,
        "time":             ["03:00"], # Este parametro pode ser fixo, pois é preciso arbitrar um horário dentre as 24 horas do dia
        "data_format":      "netcdf",
        "download_format":  "unarchived",
        "area":             RETANGULO_GEO_BRASIL # Retângulo geográfico, definido em graus, onde está o Brasil
    }
    
    client = criar_client_cds_ERA5()

    # submete a requisão e recebe um arquivo .nc contendo os dados solicitados
    ret_download = client.retrieve(dataset, request).download()

    return ret_download

def obter_periodo_data(ano: int):
    """Retorna o intervalo de datas válido para uma consulta do dataset EAC4.

    A função define o período de processamento conforme o ano informado.
    Quando o ano corresponde ao ano atual, considera apenas dados até D-2 para
    evitar atrasos de atualização dos conjuntos de reanálise. Para qualquer
    outro ano, retorna o período completo do ano civil.

    Parâmetros:
        ano (int): Ano base para a consulta.

    Retorna:
        str: String no formato 'AAAA-MM-DD/AAAA-MM-DD', representando o
        intervalo inicial e final do período de dados.

    Observações:
        - Se o parametro ano for o ano atual, o último dia considerado é sempre 2 dias
          antes da data de execução.
        - Esse ajuste é necessário porque alguns datasets podem apresentar atraso
          de até 2 dias na disponibilização dos dados.
    """
    if str(ano) == datetime.now().strftime("%Y"):
        mes             = datetime.now().strftime("%m")
        hoje            = datetime.now()
        dois_dias_atras = hoje - timedelta(days=2)
        dia             = dois_dias_atras.strftime("%d")
        return f"{ano}-01-01/{ano}-{mes}-{dia}"
    else:
        return f"{ano}-01-01/{ano}-12-31"


def recuperar_dados_EAC4(dataset_, variable, ano):
    """Baixa dados do dataset EAC4 do Climate Data Store e retorna o arquivo local.

    Parâmetros:
        dataset_ (str): Nome do dataset EAC4 a ser consultado.
        variable (str): Variável atmosférica a ser solicitada, por exemplo 'carbon_monoxide'.
        ano (int): Ano para o qual os dados devem ser baixados.

    Retorna:
        str: Caminho do arquivo .nc retornado pelo cliente CDS após o download.

    Observações:
        - A requisição fixa o nível de pressão em 1000 hPa (próximo ao nível do mar) e o horário em 03:00,
          conforme a regra adotada no módulo.
    """

    periodo_process = obter_periodo_data(ano)

    dataset = dataset_
    request = {
        "variable":       [variable],
        "pressure_level": ["1000"], # A pressão atmosférica padrão ao nível do mar é definida internacionalmente como 1013,25 hPa
        "date":           [f"{periodo_process}"],
        "time":           ["03:00"], # Este parametro pode ser fixo, pois é preciso arbitrar um horário dentre as 24 horas do dia
        "data_format":    "netcdf",
        "area":           RETANGULO_GEO_BRASIL
    }

    client = criar_client_cds_EAC4()
    ret_download = client.retrieve(dataset, request).download()
    return ret_download


def converter_netcdf4_Spark_DF(spark: SparkSession, file_name: str):
    """
    Converte um arquivo NetCDF4 em um DataFrame Spark.

    Abre o arquivo NetCDF4 usando xarray, converte o dataset em um DataFrame
    Dask, computa os dados em memória e cria um DataFrame Spark com o resultado.

    Parâmetros:
        spark: SparkSession ativa usada para criar o DataFrame Spark.
        file_name (str): Caminho completo do arquivo NetCDF4.

    Retorna:
        pyspark.sql.dataframe.DataFrame: DataFrame Spark contendo os dados do
        arquivo NetCDF4.
    """
    with xr.open_dataset(file_name
                        ,engine="netcdf4"
                        ) as ds:
        
        # Converte o dataset para Pandas DataFrame de forma limpa e reseta os índices
        pdf = ds.to_dataframe().reset_index()

        # O Spark 3.+ otimiza essa conversão usando Arrow nativamente
        df_spark = spark.createDataFrame(pdf)

    return df_spark



### Processamento LOCAL ###


# Está função Não será necessária no ambiente Databricks
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


    # Configura as variáveis do Hadoop
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"


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

# Somente para processamento LOCAL, pode ser descartada
def write_data_csv(df_write, write_path, file_name):
    
    df_write.toPandas().to_csv(f"{write_path}\{file_name}", index=False)
