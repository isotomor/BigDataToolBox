"""
Script main de ejecución que contiene el orquestador de los distintos ficheros run del proyecto.
"""
import sys

from .init_config import launcher
from .run_tablon import run_tablon

JOBS = dict(
    JOB_RUN_TABLON=dict(function=run_tablon, init_spark=True, use_databricks_spark=True)
)


def main():
    # Obtenemos el nombre de la función como parámetro de entrada
    try:
        job_function = sys.argv[1]
        function = JOBS.get(job_function)
    except IndexError:
        raise Exception("Es necesario ")

    # Lanzamos la ejecución de la función.
    launcher(func=function["function"], init_spark=function["init_spark"],
             use_databricks_spark=function["use_databricks_spark"])


if __name__ == "__main__":
    main()
