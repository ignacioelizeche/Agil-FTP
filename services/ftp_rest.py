from typing import Optional

from fastapi import APIRouter, FastAPI, HTTPException, Query
from pydantic import BaseModel

from services.ftp_manager import manager

# Router that can be included in other apps
router = APIRouter()

# Also provide a FastAPI app for standalone use
app = FastAPI(title="FTP Task Service")


class ConnectionOptions(BaseModel):
    filename_startswith: Optional[list] = None
    from_date: Optional[str] = None
    port: Optional[int] = None
    conn_type: Optional[str] = "sftp"


class ConnectionRequest(BaseModel):
    host: str
    username: str
    password: str
    directory: Optional[str] = "."
    download_options: Optional[ConnectionOptions] = None


@router.post("/utilftpget")
def utilftpget(req: ConnectionRequest):
    try:
        pid = manager.utilftpget(req.dict())
        return {"process_id": pid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/utilftpgetstatus/{pid}")
def utilftpgetstatus(pid: int):
    try:
        status = manager.utilftpgetstatus(pid)
        return {"status": status}
    except KeyError:
        raise HTTPException(status_code=404, detail="Process id not found")


@router.get("/utilftpgetlistfiles/{pid}")
def utilftpgetlistfiles(pid: int):
    try:
        files = manager.utilftpgetlistfiles(pid)
        return {"files": files}
    except KeyError:
        raise HTTPException(status_code=404, detail="Process id not found")


@router.get("/utilftpgetfile/{pid}")
def utilftpgetfile(pid: int, filename: str = Query(...)):
    try:
        b64 = manager.utilftpgetfile(pid, filename)
        return {"filename": filename, "base64": b64}
    except KeyError:
        raise HTTPException(status_code=404, detail="Process id not found")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")


@router.delete("/utilftpget/{pid}")
def utilftpgetdelete(pid: int):
    try:
        manager.utilftpgetdelete(pid)
        return {"deleted": True}
    except KeyError:
        raise HTTPException(status_code=404, detail="Process id not found")


# also include router into the standalone app so /docs on this app works
app.include_router(router)
