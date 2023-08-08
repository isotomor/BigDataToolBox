import yaml
import json
import os
import logging
from logging.handlers import HTTPHandler
from datetime import datetime
from databricks.connect import DatabricksSession


from .lake_io import create_spark_session
from .project_data import ProjectData
from .config import config_dict

NAME_CONFIG = "config.yaml"


def get_root_path_directory():
    """
    Get the root path directory
    """
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def create_logger(log_name: str, log_folder: str = None, filename: str = None,
                  log_format: str = "%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s"):
    """
    Crea un logger para registrar la ejecución

    :param log_name: ruta donde guardar los logs
    :param log_folder: nombre de los lgos (se le añadirá la fecha)
    :param filename: nombnre del archivo del log
    :param log_format: formato por defecto de los logs
    :param slack_channel: canal donde escribir los logs
    :return: logger
    slack_channel
    """
    logging.basicConfig(level=logging.INFO, format=log_format)
    logger = logging.getLogger(log_name)
    formatter = logging.Formatter(log_format)

    # Archivo de salida
    if (filename and not log_folder) or (not filename and log_folder):
        raise ValueError("Error. Si se especifica fichero o carpeta deben especificarse ambos")
    if filename:
        # Comprobamos que existe la carpeta de logs
        if not os.path.isdir(log_folder):
            os.makedirs(log_folder)

        now = str(datetime.now())[:-7]  # (yyyy-mm-dd hh:MM:ss)
        now = now.replace(":", "")
        now = now.replace(" ", "_")
        now = now.replace("-", "_")
        filename = now + "_" + filename + ".log"
        file_path = os.path.join(log_folder, filename)
        handler = logging.FileHandler(file_path)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def init_spark_configuration(logger, config):
    """
    Inicializa la sesión de spark y el ejecutor asíncrono

    :param logger: Logger
    :param config: Diccionario de configuración
    :return: Spark, async_runner
    """
    # Init Spark
    logger.info("Creating spark session...")
    spark = create_spark_session(config["SPARK_APPLICATION_NAME"])
    logger.info("Spark session created")
    return spark


def get_spark_databricks(logger):
    """
    Obtiene la sesión de databricks

    :param logger: Logger
    :param config: Diccionario de configuración
    :return: Spark, async_runner
    """
    # Init Spark
    # databricks_cluster_id = os.getenv("DATABRICKS_CLUSTER_ID", "0806-072732-s6vysj0v")
    # databricks_instance_name = os.getenv("DATABRICKS_INSTANCE_NAME", "adb-3983330658043373.13.azuredatabricks.net")
    # databricks_token = os.getenv("DATABRICKS_TOKEN", "dapicb018c60b0cb4ea3102cfcfb4b750196-2")

    databricks_cluster_id = os.getenv("DATABRICKS_CLUSTER_ID", "")
    databricks_instance_name = os.getenv("DATABRICKS_INSTANCE_NAME", "")
    databricks_token = os.getenv("DATABRICKS_TOKEN", "")

    try:
        assert databricks_cluster_id
        assert databricks_instance_name
        assert databricks_token

    except AssertionError:
        logger.error("Para usar spark databricks son necesarias las variables de entorno")

    logger.info("Getting spark session...")
    'https://adb-3983330658043373.13.azuredatabricks.net/'
    'https://adb-3983330658043373.13.azuredatabricks.net'
    spark = DatabricksSession.builder.remote(
        host=f"https://{databricks_instance_name}",
        token=databricks_token,
        cluster_id=databricks_cluster_id
    ).getOrCreate()

    logger.info("Spark session created")

    return spark

def search_config_file(folder, config_file_path, levels=3):
    """
     Busca un archivo en ./ ./src, ../ y ../../
     Siendo folder la referencia
    """
    # Comprueba si src está un directorio por debajo
    if os.path.isdir(os.path.join(folder, 'src')):
        conf_file_path = os.path.join(folder, 'src', config_file_path)

        if os.path.exists(conf_file_path):
            return conf_file_path

    # Busca en su ruta y 2 niveles anteriores
    search_folder = folder
    for level in range(levels):
        conf_file_path = os.path.join(search_folder, config_file_path)

        if os.path.exists(conf_file_path):
            return conf_file_path

        if search_folder.replace('/', '') != 'home':
            search_folder = '/'.join(search_folder.split('/')[:-1])


def get_gcp_credentials_file(folder, config_file_name, levels=3):
    """
    Devuelve la ruta del archivo de credenciales de GCP, sino string vacio
    """
    config_file_path = search_config_file(folder, config_file_name, levels)
    if config_file_path:
        return config_file_path
    else:
        return ''


def init_configuration(init_spark=False, use_databricks_spark=False, use_google_cloud=False):
    """
    Función que inicializa la configuración del proyecto.

    init_spark: Si va a True inicializa una sessión de spark y coge la configuración de config_dict
    use_databricks_spark: Si va a True usa la sesión de spark de Databrkics.
    """
    # Lee la configuración del paquete
    config = config_dict

    logger = create_logger(log_name=config["LOGGER_PREFIX"], log_folder=config["LOGGER_PATH"],
                           filename=config["LOG_FILENAME"], log_format=config["LOG_FORMAT"])

    logger.info("Cargamos el fichero de configuración:")
    for clave in config:
        logger.debug(f"{clave}: {config[clave]}")

    # SESIONES DE BBDD si procede
    if use_databricks_spark:
        spark = get_spark_databricks(logger=logger)
    elif init_spark:
        spark = init_spark_configuration(logger=logger, config=config)
    else:
        spark = None

    project_data = ProjectData(config=config, logger=logger, spark=spark)

    if use_google_cloud:
        # Guardamos la ruta del service account de Google
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = get_gcp_credentials_file(os.environ["ROOT_PATH"],
                                                                                config["SERVICE_ACCOUNT_PYTHON"])

    return project_data

