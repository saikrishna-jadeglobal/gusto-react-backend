from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers.equity import router as equity_router
from routers.approvals import router as approvals_router
from routers.health import router as health_router

app = FastAPI(title="Gusto Financial Close Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(equity_router)
app.include_router(approvals_router)

@app.get("/")
def root():
    return {"status": "running"}
