#!/usr/bin/env python3
from fastapi import APIRouter, Body, Request, Response, HTTPException, status
from fastapi.encoders import jsonable_encoder
from typing import List
from datetime import datetime
from pymongo import TEXT

from mongomodel import Tour, User, ToursUpdate, UserUpdate

router = APIRouter()

@router.post("/T", response_description="Post a new Tour", status_code=status.HTTP_201_CREATED, response_model=Tour)
def create_tour(request: Request, tour: Tour = Body(...)):
    tour = jsonable_encoder(tour)
    tour["end_date"] = datetime.strptime(tour["end_date"], "%Y-%m-%dT%H:%M:%S.%f")
    tour["start_date"] = datetime.strptime(tour["start_date"], "%Y-%m-%dT%H:%M:%S.%f")
    new_tour = request.app.database["tours"].insert_one(tour)
    created_tour = request.app.database["tours"].find_one(
        {"_id": new_tour.inserted_id}
    )

    return created_tour
@router.post("/U", response_description="Post a new User", status_code=status.HTTP_201_CREATED, response_model=User)
def create_user(request: Request, user: User = Body(...)):
    user = jsonable_encoder(user)
    new_user = request.app.database["users"].insert_one(user)
    created_user = request.app.database["users"].find_one(
        {"_id": new_user.inserted_id}
    )

    return created_user

@router.get("/U", response_description="Get all Users", response_model=List[User])
def list_users(request: Request, limit: int = 0, skip: int = 0):
    req={}
    if limit <=0:
        if skip  <=0:
            users = list(request.app.database["users"].find(req))
        else:
            users = list(request.app.database["users"].find(req).skip(skip))
    else:
        if skip  <=0:
            users = list(request.app.database["users"].find(req).limit(limit))
        else:
            users = list(request.app.database["users"].find(req).skip(skip).limit(limit))
    return users

@router.get("/T", response_description="Get all tours", response_model=List[Tour])
def list_tours(request: Request, start_date_From: str=None, start_date_To: str=None, min_price: float=None, max_price: float=None, location: str=None):
    request.app.database["tours"].create_index([("start_date", 1)])
    request.app.database["tours"].create_index([("price_per_person", 1)])
    request.app.database["tours"].create_index([("location", TEXT)])
    req={}
    if start_date_From!=None and start_date_To!=None:
        start_date_From=datetime.strptime(start_date_From, "%Y-%m-%d %H:%M:%S.%f")
        start_date_To=datetime.strptime(start_date_To, "%Y-%m-%d %H:%M:%S.%f")
        req['start_date']={"$gte": start_date_From,"$lte": start_date_To}
    elif min_price!=None and max_price!=None:
        req['price_per_person']={"$gte": min_price,"$lte": max_price}
    elif location!=None:
            req['$text']={"$search": location}

    tours = list(request.app.database["tours"].find(req))
    return tours

@router.get("/T/{id}", response_description="Get a single tour by id", response_model=Tour)
def find_tour(id: str, request: Request):
    if (tour := request.app.database["tours"].find_one({"_id": id})) is not None:
        return tour

    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Tour with ID {id} not found")


@router.put("/T/{id}", response_description="Update a tour by id", response_model=Tour)
def update_tour(id: str, request: Request, tour: ToursUpdate = Body(...)):
    new_data = tour.dict(exclude_unset=True)
    request.app.database["tours"].update_one({"_id": id}, {"$set": new_data})
    updated_tour = request.app.database["tours"].find_one({"_id": id})
    return updated_tour


@router.delete("/T/{id}", response_description="Delete a tour")
def delete_tour(id: str, request: Request):
    request.app.database["tours"].delete_one({"_id": id})
    return Response(status_code=status.HTTP_204_NO_CONTENT)