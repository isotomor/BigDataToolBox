def suma(x, y):
    return x + y

def resta(x, y):
    return x + y


def calculadora_hip(volumen, interes_anual, anios):
    '''
    Función que calcula la cuota mensual de una hipoteca

    Args:
        volumen (float): El volumen total de la hipoteca en euros
        interes_anual (float): Tasa de interés anual
        anios (int): Número de años para liquidar la hipoteca
    Return:
        cuota (float): Cuota mensual para la hipoteca
    '''
    n_meses = anios * 12
    interes = interes_anual / 100 / 12
    cuota = volumen * ((1 + interes) ** n_meses) * interes / (((1 + interes) ** n_meses) - 1)
    return cuota

