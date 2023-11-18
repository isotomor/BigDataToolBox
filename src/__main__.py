"""
Script main de ejecución que contiene el orquestador de los distintos ficheros run del proyecto.
"""
import sys

from src.init_config import launcher
from src.run_tablon import run_tablon
from src.run_tablon_analytics import run_tablon_analytics
from src.models.main_baseline_prophet_cval import run_main_baseline_prophet_cval


JOBS = dict(
    JOB_RUN_TABLON=dict(function=run_tablon, init_spark=True, use_databricks_spark=True),
    JOB_PROPHET_CVAL=dict(function=run_main_baseline_prophet_cval, init_spark=True, use_databricks_spark=True),
    JOB_RUN_TABLON_ANALYTICS=dict(function=run_tablon_analytics, init_spark=True, use_databricks_spark=True)
)


def main():
    print("Iniciando proceso...")
    # Obtenemos el nombre de la función como parámetro de entrada
    job_function = sys.argv[1]
    enviroment = sys.argv[2] if len(sys.argv) > 2 else 'DESARROLLO'

    try:
        function = JOBS.get(job_function)
    except IndexError:
        raise Exception("Es necesario el argumento de la función a ejecutar")

    # Lanzamos la ejecución de la función.
    launcher(func=function["function"], init_spark=function["init_spark"],
             use_databricks_spark=function["use_databricks_spark"],
             enviroment=enviroment)


if __name__ == "__main__":
    main()
