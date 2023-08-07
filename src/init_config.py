
import os
from collections.abc import Callable
from bigdatatoolbox.init_config import init_configuration, _get_config

NAME_CONFIG = "config.yaml"


def launcher(func: Callable, init_spark=False, use_databricks_spark=False, **kwargs):
    """
    Función que lanza las funciones pasadas como parámetros entrada.
    A todas las funciones les crea un config y un logger.
    """
    root_path = os.getcwd() if '/src' not in os.getcwd() else os.getcwd().split('/src')[0]


    # Configuración del programa
    run_config = _get_config(folder=root_path, config_file_name=NAME_CONFIG)

    # Configuración del DataLake
    project_data = init_configuration(init_spark=init_spark, use_databricks_spark=use_databricks_spark,
                                      program_config=run_config)

    func(project_data, **kwargs)

