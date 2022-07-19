from typing import List

from pydantic import BaseModel


class GetDataResponse(BaseModel):
    data: List[str]
