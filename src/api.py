from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def read_root():
    return {"message": "Hello from the router!"}

@router.get("/items/{item_id}")
async def read_item(item_id: int):
    return {"item_id": item_id, "message": "This is an item from the src router"}
