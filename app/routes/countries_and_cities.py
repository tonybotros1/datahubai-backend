import logging
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Form, File, UploadFile, Body, Depends
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.widgets import upload_images
from app.websocket_config import manager
from pymongo import ReturnDocument

router = APIRouter()
countries_collection = get_collection("all_countries")
cities_collection = get_collection("all_countries_cities")
logging.basicConfig(level=logging.INFO)


def country_serializer(country: dict) -> dict:
    country["_id"] = str(country["_id"])
    for key, value in country.items():
        if isinstance(value, datetime):
            country[key] = value.isoformat()
    return country


def city_serializer(city: dict) -> dict:
    city["_id"] = str(city["_id"])
    for key, value in city.items():
        if isinstance(value, datetime):
            city[key] = value.isoformat()
    city["country_id"] = str(city["country_id"])
    return city


# ====================================== Countries Section ============================================

# this is to get all countries from database
@router.get("/get_countries")
async def get_countries(_: dict = Depends(security.get_current_user)):
    countries = await countries_collection.find().sort("name", 1).to_list()
    return {"countries": [country_serializer(c) for c in countries]}


# this is to add new country
@router.post("/add_new_country")
async def add_new_country(name: str = Form(...), code: str = Form(...), calling_code: str = Form(...),
                          currency_name: str = Form(...), currency_code: str = Form(...),
                          vat: float = Form(...),
                          flag: UploadFile = File(...), _: dict = Depends(security.get_current_user)):
    try:
        result = await upload_images.upload_image(flag, "countries")
        country_dict = {
            "name": name,
            "code": code,
            "currency_name": currency_name,
            "currency_code": currency_code,
            "vat": vat,
            "flag": result["url"],
            "flag_public_id": result["public_id"],
            "calling_code": calling_code,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
            "status": True
        }
        insert_result = await countries_collection.insert_one(country_dict)
        country_dict["_id"] = str(insert_result.inserted_id)
        serialized = country_serializer(country_dict)

        await manager.broadcast({
            "type": "country_added",
            "data": serialized
        })
        return {"message": "Country added successfully!", "country": serialized}


    except Exception as e:
        return {"error": str(e)}


# this is to change country status active / inactive
@router.patch("/change_country_status/{country_id}")
async def change_country_status(country_id: str, status: bool = Body(..., embed=True),
                                _: dict = Depends(security.get_current_user)):
    try:
        # logging.info(f"Changing status of {country_id} to {status}")
        updated_country = await countries_collection.find_one_and_update(
            {"_id": ObjectId(country_id)},
            {"$set": {"status": status, "updatedAt": datetime.now(timezone.utc)}},
            return_document=ReturnDocument.AFTER
        )

        if not updated_country:
            raise HTTPException(status_code=404, detail="Country not found")

        serialized = country_serializer(updated_country)

        await manager.broadcast({
            "type": "country_updated",
            "data": serialized
        })

        return {"message": "Country updated successfully!", "Country": serialized}


    except Exception as e:
        return {"error": str(e)}


from fastapi import HTTPException, Depends
from bson import ObjectId


@router.delete("/delete_country/{country_id}")
async def delete_country(country_id: str, _: dict = Depends(security.get_current_user)):
    # Find the country first
    country = await countries_collection.find_one({"_id": ObjectId(country_id)})
    if not country:
        raise HTTPException(status_code=404, detail="Country not found")

    try:
        # If country has a flag, try to delete it
        if country.get("flag") and country.get("flag_public_id"):
            await upload_images.delete_image_from_server(country["flag_public_id"])

        # Delete the country document
        result = await countries_collection.delete_one({"_id": ObjectId(country_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Country not found")

        # Broadcast deletion event
        await manager.broadcast({
            "type": "country_deleted",
            "data": {"_id": country_id}
        })

        return {"message": "Country deleted successfully!"}

    except Exception as e:
        print(f"Error deleting country: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete country")


# This is to update country
@router.patch("/update_country/{country_id}")
async def update_country(country_id: str, name: str | None = Form(None), flag: UploadFile | None = File(None),
                         calling_code: str = Form(None), currency_name: str = Form(None),
                         currency_code: str = Form(None),
                         vat: float = Form(None), _: dict = Depends(security.get_current_user)):
    try:
        country = await countries_collection.find_one({"_id": ObjectId(country_id)})

        if not country:
            raise HTTPException(status_code=404, detail="Country not found")

        if name:
            country["name"] = name

        if flag:
            if country['flag'] and country['flag_public_id']:
                await upload_images.delete_image_from_server(country['flag_public_id'])
            result = await upload_images.upload_image(flag, 'countries')
            country["flag"] = result["url"]
            country["flag_public_id"] = result["public_id"]

        if calling_code:
            country["calling_code"] = calling_code

        if currency_name:
            country["currency_name"] = currency_name
        if currency_code:
            country["currency_code"] = currency_code

        if vat:
            country["vat"] = vat

        country["updatedAt"] = datetime.now(timezone.utc)
        await countries_collection.update_one({"_id": ObjectId(country_id)}, {"$set": country})

        serialized = country_serializer(country)

        await manager.broadcast({
            "type": "country_updated",
            "data": serialized
        })

        return {"message": "Country updated successfully!", "Country": serialized}


    except Exception as e:
        return {"error": str(e)}


# ====================================== Cities Section ============================================

# this is to get all cities
@router.get("/get_cities/{country_id}")
async def get_cities(country_id: str, _: dict = Depends(security.get_current_user)):
    cities = await cities_collection.find({"country_id": ObjectId(country_id)}).sort("name", 1).to_list()
    return {"cities": [city_serializer(c) for c in cities]}


# this is to add new city to country
@router.post("/add_new_city/{country_id}")
async def add_new_city(country_id: str, name: str = Body(..., embed=True), code: str = Body(..., embed=True),
                       _: dict = Depends(security.get_current_user)):
    city_dict = {
        "name": name,
        "code": code,
        "updatedAt": datetime.now(timezone.utc),
        "createdAt": datetime.now(timezone.utc),
        "country_id": ObjectId(country_id),
        "available ": True
    }
    result = await cities_collection.insert_one(city_dict)
    city_dict["_id"] = str(result.inserted_id)
    serialized = city_serializer(city_dict)

    await manager.broadcast({
        "type": "city_added",
        "data": serialized
    })
    return {"message": "City added successfully!", "City": serialized}


# this is to change city's status active / inactive
@router.patch("/change_city_status/{city_id}")
async def change_city_status(city_id: str, status: bool = Body(..., embed=True),
                             _: dict = Depends(security.get_current_user)):
    try:
        updated_city = await cities_collection.find_one_and_update(
            {"_id": ObjectId(city_id)},
            {"$set": {"available": status, "updatedAt": datetime.now(timezone.utc)}},
            return_document=ReturnDocument.AFTER
        )

        if not updated_city:
            raise HTTPException(status_code=404, detail="City not found")

        serialized = city_serializer(updated_city)

        await manager.broadcast({
            "type": "city_updated",
            "data": serialized
        })

        return {"message": "Country updated successfully!", "Country": serialized}


    except Exception as e:
        return {"error": str(e)}


# This is to update country
@router.patch("/update_city/{city_id}")
async def update_city(city_id: str, name: str | None = Form(None),
                      code: str = Form(None), _: dict = Depends(security.get_current_user)):
    try:
        city = await cities_collection.find_one({"_id": ObjectId(city_id)})

        if not city:
            raise HTTPException(status_code=404, detail="City not found")

        if name:
            city["name"] = name

        if code:
            city["code"] = code

        city["updatedAt"] = datetime.now(timezone.utc)
        await cities_collection.update_one({"_id": ObjectId(city_id)}, {"$set": city})

        serialized = city_serializer(city)

        await manager.broadcast({
            "type": "city_updated",
            "data": serialized
        })

        return {"message": "City updated successfully!", "City": serialized}


    except Exception as e:
        return {"error": str(e)}


# this is to delete city
@router.delete("/delete_city/{city_id}")
async def delete_city(city_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await cities_collection.delete_one({"_id": ObjectId(city_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="City not found")
        else:
            await manager.broadcast({
                "type": "city_deleted",
                "data": {"_id": city_id}
            })
            return {"message": "City deleted successfully"}
    except Exception as e:
        return {"error": str(e)}
