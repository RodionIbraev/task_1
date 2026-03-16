from fastapi import FastAPI, Request

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello from upstream"}


@app.get("/{path:path}")
async def echo_get(path: str, request: Request):
    return {
        "method": request.method,
        "path": path,
        "headers": dict(request.headers),
    }


@app.post("/{path:path}")
async def echo_post(path: str, request: Request):
    body = await request.body()

    return {
        "method": request.method,
        "path": path,
        "headers": dict(request.headers),
        "body": body.decode(errors="ignore"),
    }
