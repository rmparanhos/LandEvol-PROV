from pydantic import BaseModel
from typing import Union

class Filter(BaseModel):
    attribute: str
    operand: str
    value: Union[float, int, str]