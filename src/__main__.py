"""
Script main de ejecución que contiene el orquestador de los distintos ficheros run del proyecto.
"""
import sys

from .init_config import launcher
from .run_tablon import run_tablon
from .models.main_baseline_prophet_cval import run_main_baseline_prophet_cval

JOBS = dict(
    JOB_RUN_TABLON=dict(function=run_tablon, init_spark=True, use_databricks_spark=True),
    JOB_PROPHET_CVAL=dict(function=run_main_baseline_prophet_cval, init_spark=True, use_databricks_spark=True)
)


def main(function_main, enviroment_main):
    try:
        function = JOBS.get(function_main)
    except IndexError:
        raise Exception("Es necesario el argumento de la función a ejecutar")

    # Lanzamos la ejecución de la función.
    launcher(func=function["function"], init_spark=function["init_spark"],
             use_databricks_spark=function["use_databricks_spark"],
             enviroment=enviroment_main)


if __name__ == "__main__":

    # Obtenemos el nombre de la función como parámetro de entrada
    job_function = sys.argv[1]
    enviroment = sys.argv[2] if len(sys.argv) > 2 else 'DESARROLLO'

    main(function_main=function, enviroment_main=enviroment)
