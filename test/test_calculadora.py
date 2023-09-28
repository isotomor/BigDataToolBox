from calculadora import suma, resta
import os



def test_suma():
    # Mi test
    expected_return_str = 12

    return_str = suma(5, 7)

    assert expected_return_str == return_str


def test_resta():
    expected_return_str = -2

    return_str = resta(5, 7)

    assert expected_return_str == return_str
