from pydantic import BaseModel,EmailStr,HttpUrl


class NLPDataInput(BaseModel):
    text: list[str]
    user_id: EmailStr

class NLPDataOutput(BaseModel):
    model: str
    text: list[str]
    target: list[str]
    score: list[float]
    prediction_time: float


class ImageDataInput(BaseModel):
    url: list[str]
    user_id: EmailStr

class ImageDataOutput(BaseModel):
    model: str
    url: list[str]
    target: list[str]
    score: list[float]
    prediction_time: float




