import pytest 

@pytest.fixture
def sampleData():
    return [1,2,3,4]

def test_sum(sampleData):
    assert sum(sampleData) == 10

@pytest.fixture
def base():
    return 7

@pytest.mark.parametrize("power,expected", [(2,4),(3,8), (5,6)])
def test_exponentiation(base,power,expected):
    assert base**power==expected