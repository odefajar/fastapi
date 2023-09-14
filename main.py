from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class Post(BaseModel):
    title: str
    content: str
    published: bool = True
    rating: Optional[int] = None


@app.get("/")
def root():
    return {"message": "Hello my man"}


@app.get("/posts")
def get_posts():
    return {"data": "This is your data"}


@app.post("/createposts")
def create_posts(post: Post):
    print(post.dict())
    return {"new_post": post}
