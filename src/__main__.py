"""
Script main de ejecución que contiene el orquestador de los distintos ficheros run del proyecto.
"""
import sys

from .init_config import launcher
from .run_tablon import run_tablon
from .models.main_baseline_prophet_cval import run_main_baseline_prophet_cval

JOBS = dict(
    JOB_RUN_TABLON=dict(function=run_tablon, init_spark=True, use_databricks_spark=True),
    JOB_PROPHET_CVAL=dict(function=run_main_baseline_prophet_cval, init_spark=False, use_databricks_spark=False)
)


def main():
    # Obtenemos el nombre de la función como parámetro de entrada
    try:
        job_function = sys.argv[1]
        function = JOBS.get(job_function)
    except IndexError:
        raise Exception("Es necesario el argumento de la función a ejecutar")

    if len(sys.argv) > 2:
        enviroment = sys.argv[2]

    # Lanzamos la ejecución de la función.
    launcher(func=function["function"], init_spark=function["init_spark"],
             use_databricks_spark=function["use_databricks_spark"], enviroment=enviroment)


if __name__ == "__main__":
    main()
