# Script que ejecuta python wheel
import sys
from src.__main__ import main

if __name__ == "__main__":

    # Obtenemos el nombre de la función como parámetro de entrada
    job_function = sys.argv[1]
    enviroment = sys.argv[2] if len(sys.argv) > 2 else 'DESARROLLO'

    main(function_main=job_function, enviroment_main=enviroment)
