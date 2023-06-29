
import os
from collections.abc import Callable
from edh_functions.init_config import init_configuration, _get_config

NAME_CONFIG = "config.yaml"


def launcher(func: Callable, init_spark=False, init_azure_sql=False, production_execution=False, password_azure=None,
             config_run_spark=None, **kwargs):
    """
    Función que lanza las funciones pasadas como parámetros entrada.
    A todas las funciones les crea un config y un logger.
    """
    root_path = os.getcwd() if '/src' not in os.getcwd() else os.getcwd().split('/src')[0]
    # sys.path.extend([root_path, os.path.join(root_path, 'edh_functions')])

    # Directorio raiz del proyecto
    os.environ["ROOT_PATH"] = root_path
    os.environ["PRO_ENV"] = "T" if production_execution else "F"
    os.environ['TZ'] = 'Europe/Madrid'

    # Configuración del programa
    run_config = _get_config(folder=root_path, config_file_name=NAME_CONFIG)

    # Configuración del DataLake
    project_data = init_configuration(init_spark=init_spark, init_azure_sql=init_azure_sql,
                                      production_execution=production_execution, password_azure=password_azure,
                                      config_run_spark=config_run_spark, program_config=run_config)

    func(project_data, **kwargs)

