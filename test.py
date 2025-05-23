

import pandas as pd
from fastapi import FastAPI, HTTPException

def factorial(n):
    return 1 if n == 0 else n * factorial(n-1)

def palindrome(s):
    return s == s[::-1]

def test_factorial():
    assert factorial(0) == 1
    assert factorial(1) == 1
    assert factorial(2) == 2
    assert factorial(3) == 6
    assert factorial(4) == 24
    assert factorial(5) == 120

"""class api:
    def __init__(self):
        self.app = FastAPI()

    @self.app.get("/")
    def read_root():
        return {"Hello": "World"}

    @self.app.get("/items/{item_id}")
    def read_item(item_id: int, q: str = None):
        return {"item_id": item_id, "q": q}

    @self.app.post("/items/")
    def create_item(item: dict):
        return item"""

if __name__ == "__main__":

    d1 = pd.DataFrame({ 
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "c": [7, 8, 9]
    })

    d2 = pd.DataFrame({ 
        "a": [10, 11, 12],
        "b": [13, 14, 15],
        "c": [16, 17, 18]
    })  

    d3 = pd.concat([d1, d2], axis=0, ignore_index=True)  

    test_factorial()
    print(factorial(10))
    print("All tests passed!")
    s = dict()
    s["a"] = 1
    d = {"a": 1, "b": 2}
    print(d)
    print(s)
    print(d3)