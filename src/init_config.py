
import os
from collections.abc import Callable
from .bigdatatoolbox.init_config import init_configuration

NAME_CONFIG = "config.yaml"


def launcher(func: Callable, init_spark=False, use_databricks_spark=False, enviroment='DESARROLLO', **kwargs):
    """
    Función que lanza las funciones pasadas como parámetros entrada.
    A todas las funciones les crea un config y un logger.
    """
    # Configuración del DataLake
    project_data = init_configuration(init_spark=init_spark, use_databricks_spark=use_databricks_spark,
                                      enviroment=enviroment)

    func(project_data, **kwargs)

