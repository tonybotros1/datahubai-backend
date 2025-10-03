from typing import Optional, Dict, Any
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Body, Form, UploadFile, File
from pydantic import BaseModel
import json
from app.core import security
from app.database import get_collection
from datetime import datetime

from app.websocket_config import manager
from app.widgets import upload_images
from app.widgets.upload_images import upload_image

router = APIRouter()
entity_information_collection = get_collection("entity_information")


def serializer(doc: dict) -> dict:
    def convert(value):
        if isinstance(value, ObjectId):
            return str(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, list):
            return [convert(v) for v in value]
        elif isinstance(value, dict):
            return {k: convert(v) for k, v in value.items()}
        return value

    return {k: convert(v) for k, v in doc.items()}


pipeline: list[Dict[str, Any]] = [
    {
        '$lookup': {
            'from': 'sales_man',
            'let': {
                'salesman_id': '$salesman_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$salesman_id'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'name': 1
                    }
                }
            ],
            'as': 'salesman_lookup'
        }
    }, {
        '$addFields': {
            'salesman_id': {
                '$ifNull': [
                    {
                        '$arrayElemAt': [
                            '$salesman_lookup._id', 0
                        ]
                    }, None
                ]
            },
            'salesman': {
                '$ifNull': [
                    {
                        '$arrayElemAt': [
                            '$salesman_lookup.name', 0
                        ]
                    }, None
                ]
            }
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'industry_id': '$industry_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$industry_id'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'name': 1
                    }
                }
            ],
            'as': 'industry_lookup'
        }
    }, {
        '$addFields': {
            'industry_id': {
                '$ifNull': [
                    {
                        '$arrayElemAt': [
                            '$industry_lookup._id', 0
                        ]
                    }, None
                ]
            },
            'industry': {
                '$ifNull': [
                    {
                        '$arrayElemAt': [
                            '$industry_lookup.name', 0
                        ]
                    }, None
                ]
            }
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'entity_type_id': '$entity_type_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$entity_type_id'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'name': 1
                    }
                }
            ],
            'as': 'entity_type_lookup'
        }
    }, {
        '$addFields': {
            'entity_type_id': {
                '$ifNull': [
                    {
                        '$arrayElemAt': [
                            '$entity_type_lookup._id', 0
                        ]
                    }, None
                ]
            },
            'entity_type': {
                '$ifNull': [
                    {
                        '$arrayElemAt': [
                            '$entity_type_lookup.name', 0
                        ]
                    }, None
                ]
            }
        }
    }, {
        '$lookup': {
            'from': 'all_countries',
            'let': {
                'country_ids': '$entity_address.country_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', '$$country_ids'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'name': 1
                    }
                }
            ],
            'as': 'country_lookup'
        }
    }, {
        '$lookup': {
            'from': 'all_countries_cities',
            'let': {
                'city_ids': '$entity_address.city_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', '$$city_ids'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'name': 1
                    }
                }
            ],
            'as': 'city_lookup'
        }
    }, {
        '$addFields': {
            'entity_address': {
                '$map': {
                    'input': '$entity_address',
                    'as': 'addr',
                    'in': {
                        'line': '$$addr.line',
                        'isPrimary': '$$addr.isPrimary',
                        'country_id': '$$addr.country_id',
                        'country': {
                            '$ifNull': [
                                {
                                    '$arrayElemAt': [
                                        {
                                            '$map': {
                                                'input': {
                                                    '$filter': {
                                                        'input': '$country_lookup',
                                                        'cond': {
                                                            '$eq': [
                                                                '$$this._id', '$$addr.country_id'
                                                            ]
                                                        }
                                                    }
                                                },
                                                'as': 'c',
                                                'in': '$$c.name'
                                            }
                                        }, 0
                                    ]
                                }, None
                            ]
                        },
                        'city_id': '$$addr.city_id',
                        'city': {
                            '$ifNull': [
                                {
                                    '$arrayElemAt': [
                                        {
                                            '$map': {
                                                'input': {
                                                    '$filter': {
                                                        'input': '$city_lookup',
                                                        'cond': {
                                                            '$eq': [
                                                                '$$this._id', '$$addr.city_id'
                                                            ]
                                                        }
                                                    }
                                                },
                                                'as': 'c',
                                                'in': '$$c.name'
                                            }
                                        }, 0
                                    ]
                                }, None
                            ]
                        }
                    }
                }
            }
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'type_ids': '$entity_phone.type_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', '$$type_ids'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'name': 1
                    }
                }
            ],
            'as': 'phone_type_lookup'
        }
    }, {
        '$addFields': {
            'entity_phone': {
                '$map': {
                    'input': '$entity_phone',
                    'as': 'phone',
                    'in': {
                        'number': '$$phone.number',
                        'name': '$$phone.name',
                        'job_title': '$$phone.job_title',
                        'email': '$$phone.email',
                        'isPrimary': '$$phone.isPrimary',
                        'type_id': '$$phone.type_id',
                        'type': {
                            '$arrayElemAt': [
                                {
                                    '$map': {
                                        'input': {
                                            '$filter': {
                                                'input': '$phone_type_lookup',
                                                'cond': {
                                                    '$eq': [
                                                        '$$this._id', '$$phone.type_id'
                                                    ]
                                                }
                                            }
                                        },
                                        'as': 'p',
                                        'in': '$$p.name'
                                    }
                                }, 0
                            ]
                        }
                    }
                }
            }
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'type_ids': '$entity_social.type_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', '$$type_ids'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'name': 1
                    }
                }
            ],
            'as': 'social_type_lookup'
        }
    }, {
        '$addFields': {
            'entity_social': {
                '$map': {
                    'input': '$entity_social',
                    'as': 'social',
                    'in': {
                        'link': '$$social.link',
                        'type_id': '$$social.type_id',
                        'type': {
                            '$arrayElemAt': [
                                {
                                    '$map': {
                                        'input': {
                                            '$filter': {
                                                'input': '$social_type_lookup',
                                                'cond': {
                                                    '$eq': [
                                                        '$$this._id', '$$social.type_id'
                                                    ]
                                                }
                                            }
                                        },
                                        'as': 's',
                                        'in': '$$s.name'
                                    }
                                }, 0
                            ]
                        }
                    }
                }
            }
        }
    }, {
        '$project': {
            '_id': 1,
            'entity_name': 1,
            'entity_code': 1,
            'entity_status': 1,
            'trn': 1,
            'credit_limit': 1,
            'group_name': 1,
            'status': 1,
            'entity_picture': 1,
            'salesman_id': 1,
            'salesman': 1,
            'industry_id': 1,
            'industry': 1,
            'entity_type_id': 1,
            'entity_type': 1,
            'entity_address': 1,
            'entity_phone': 1,
            'entity_social': 1,
            'createdAt': 1,
            'updatedAt': 1
        }
    }
]


async def get_entity_details(entity_id: ObjectId):
    new_pipeline = pipeline.copy()
    new_pipeline.insert(1, {
        "$match": {
            "_id": entity_id
        }
    })
    cursor = await entity_information_collection.aggregate(new_pipeline)
    results = await cursor.to_list(1)
    return results[0]


@router.get("/get_all_entities")
async def get_all_entities(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        new_pipeline = pipeline.copy()
        new_pipeline.insert(1, {
            "$match": {
                "company_id": company_id,
            }
        })
        cursor = await entity_information_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        return {"entities": [serializer(e) for e in results]}

    except Exception as e:
        raise e


@router.post("/add_new_entities")
async def add_new_entity(
        entity_name: str = Form(None),
        entity_code: str = Form(None),
        credit_limit: float = Form(None),
        salesman_id: str = Form(None),
        entity_status: str = Form(None),
        group_name: str = Form(None),
        industry_id: str = Form(None),
        trn: str = Form(None),
        entity_type_id: str = Form(None),
        entity_address: str = Form("[]"),
        entity_phone: str = Form("[]"),
        entity_social: str = Form("[]"),
        file: UploadFile = File(None),
        data: dict = Depends(security.get_current_user),
):
    company_id = ObjectId(data.get("company_id"))
    entity_picture = ""
    entity_picture_public_id = ""
    if file:
        result = await upload_images.upload_image(file, folder="entity_information")
        if result and not result.get("error"):
            entity_picture = result['url']
            entity_picture_public_id = result['public_id']

    try:
        entity_address: Any = json.loads(entity_address) if entity_address else []
        entity_phone: Any = json.loads(entity_phone) if entity_phone else []
        entity_social: Any = json.loads(entity_social) if entity_social else []
        if entity_address:
            for entity in entity_address:
                entity['country_id'] = ObjectId(entity['country_id']) if entity['country_id'] else None
                entity.pop('country')
                entity.pop('city')
                entity['city_id'] = ObjectId(entity['city_id']) if entity['city_id'] else None
        if entity_phone:
            for entity in entity_phone:
                entity['type_id'] = ObjectId(entity['type_id']) if entity['type_id'] else None
                entity.pop('type')

        if entity_social:
            for entity in entity_social:
                entity['type_id'] = ObjectId(entity['type_id']) if entity['type_id'] else None
                entity.pop('type')
        doc = {
            "entity_name": entity_name,
            "entity_picture": entity_picture,
            "entity_picture_public_id": entity_picture_public_id,
            "entity_code": entity_code.split(","),
            "credit_limit": credit_limit if credit_limit else 0,
            "salesman_id": ObjectId(salesman_id) if salesman_id else None,
            "entity_status": entity_status,
            "group_name": group_name,
            "industry_id": ObjectId(industry_id) if industry_id else None,
            "trn": trn,
            "entity_type_id": ObjectId(entity_type_id) if entity_type_id else None,
            "company_id": company_id,
            "entity_address": entity_address,
            "entity_phone": entity_phone,
            "entity_social": entity_social,
            "status": True,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }

        result = await entity_information_collection.insert_one(doc)
        new_entity = await get_entity_details(result.inserted_id)
        serialized = serializer(new_entity)
        await manager.broadcast({
            "type": "entity_created",
            "data": serialized
        })

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_entity/{entity_id}")
async def update_entity(entity_id: str, entity_name: str = Form(None),
                        entity_code: str = Form(None),
                        credit_limit: float = Form(None),
                        salesman_id: str = Form(None),
                        entity_status: str = Form(None),
                        group_name: str = Form(None),
                        industry_id: str = Form(None),
                        trn: str = Form(None),
                        entity_type_id: str = Form(None),
                        entity_address: str = Form("[]"),
                        entity_phone: str = Form("[]"),
                        entity_social: str = Form("[]"),
                        file: UploadFile = File(None),
                        _: dict = Depends(security.get_current_user), ):
    try:
        doc = {
            "entity_name": entity_name,
            "entity_code": entity_code.split(","),
            "credit_limit": credit_limit or 0,
            "salesman_id": ObjectId(salesman_id) if salesman_id else None,
            "entity_status": entity_status,
            "group_name": group_name,
            "industry_id": ObjectId(industry_id) if industry_id else None,
            "trn": trn,
            "entity_type_id": ObjectId(entity_type_id) if entity_type_id else None,
            "updatedAt": security.now_utc(),
        }
        if file:
            current_entity = await entity_information_collection.find_one({"_id": ObjectId(entity_id)})
            if current_entity.get("entity_picture_public_id"):
                await upload_images.delete_image_from_server(current_entity.get("entity_picture_public_id"))
            result = await upload_images.upload_image(file, folder="entity_information")
            if result and not result.get("error"):
                doc['entity_picture'] = result['url']
                doc['entity_picture_public_id'] = result['public_id']
        entity_address: Any = json.loads(entity_address) if entity_address else []
        entity_phone: Any = json.loads(entity_phone) if entity_phone else []
        entity_social: Any = json.loads(entity_social) if entity_social else []
        if entity_address:
            print(entity_address)
            for entity in entity_address:
                entity['country_id'] = ObjectId(entity['country_id']) if entity['country_id'] else None
                entity.pop('country')
                entity.pop('city')
                entity['city_id'] = ObjectId(entity['city_id']) if entity['city_id'] else None
            doc["entity_address"] = entity_address

        if entity_phone:
            for entity in entity_phone:
                entity['type_id'] = ObjectId(entity['type_id']) if entity['type_id'] else None
                entity.pop('type')
            doc["entity_phone"] = entity_phone

        if entity_social:
            for entity in entity_social:
                entity['type_id'] = ObjectId(entity['type_id']) if entity['type_id'] else None
                entity.pop('type')
            doc["entity_social"] = entity_social

        await  entity_information_collection.update_one({"_id": ObjectId(entity_id)}, {"$set": doc})
        updated_entity = await get_entity_details(ObjectId(entity_id))
        serialized = serializer(updated_entity)
        await manager.broadcast({
            "type": "entity_updated",
            "data": serialized
        })

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_entity/{entity_id}")
async def delete_entity(entity_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await entity_information_collection.find_one_and_delete({"_id": ObjectId(entity_id)})
        if result and result.get("entity_picture_public_id"):
            await upload_images.delete_image_from_server(result["entity_picture_public_id"])

        await manager.broadcast({
            "type": "entity_deleted",
            "data": {"_id": entity_id}
        })
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/change_entity_status/{entity_id}")
async def change_entity_status(entity_id: str, status: bool = Body(None), _: dict = Depends(security.get_current_user)):
    try:
        result = await entity_information_collection.update_one(
            {"_id": ObjectId(entity_id)}, {"$set": {"status": status, "updatedAt": security.now_utc()}},
        )
        if not result:
            raise HTTPException(status_code=404, detail="Entity not found")
        await manager.broadcast({
            "type": "entity_status_updated",
            "data": {"status": status, "_id": entity_id}
        })
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))
