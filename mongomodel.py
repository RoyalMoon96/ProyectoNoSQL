#!/usr/bin/env python3
import uuid
from typing import Optional
from pydantic import BaseModel, Field
import datetime
#tour_name,location,duration_days,price_per_person,start_date,max_participants,end_date
class Tour(BaseModel):
    id: str = Field(default_factory=uuid.uuid4, alias="_id")
    tour_name: str = Field(...)
    location: str = Field(...)
    duration_days: int = Field(...)
    price_per_person: float = Field(...)
    start_date: datetime.datetime = Field(...)
    end_date: datetime.datetime = Field(...)
    max_participants: int = Field(...)

    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                'tour_name': "Mysterious Mission",
                'location': "Tokyo",
                'duration_days': 4,
                'price_per_person': 1549.37,
                'start_date': "2025-09-04 09:55:17.905467",
                'max_participants':12,
                'end_date': "2025-09-04 09:55:17.905467"
            }
        }


class ToursUpdate(BaseModel):
    tour_name: Optional[str]
    location: Optional[str]
    duration_days: Optional[int]
    price_per_person: Optional[float]
    start_date: Optional[datetime.datetime]
    end_date: Optional[datetime.datetime]
    max_participants: Optional[int]

    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                'tour_name': "Mysterious Mission",
                'location': "Tokyo",
                'duration_days': 4,
                'price_per_person': 1549.37,
                'start_date': "2025-04-08T09:55:17.940000",
                'max_participants':12,
                'end_date': "2025-04-08T09:55:17.940000"
            }
        }

#username,age,state,real_name,email
class User(BaseModel):
    id: str = Field(default_factory=uuid.uuid4, alias="_id")
    username: str = Field(...)
    age: int = Field(...)
    state: str = Field(...)
    real_name: str = Field(...)
    email: str = Field(...)

    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                'username': "xtreme_warrior",
                'age': 55,
                'state': "Nevada",
                'real_name': "Ali Rios",
                'email': "xtreme_warrior@live.com"
            }
        }


class UserUpdate(BaseModel):
    username: Optional[str]
    age: Optional[int]
    state: Optional[str]
    real_name: Optional[str]
    email: Optional[str]

    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                'username': "xtreme_warrior",
                'age': 55,
                'state': "Nevada",
                'real_name': "Ali Rios",
                'email': "xtreme_warrior@live.com"
            }
        }