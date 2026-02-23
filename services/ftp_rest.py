from typing import Optional

from fastapi import APIRouter, FastAPI, HTTPException, Query
from fastapi.responses import Response
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

class PIDRequest(BaseModel):
    pid: int

class FileRequest(BaseModel):
    pid: int
    filename: str


@router.post("/utilftpget")
def utilftpget(req: ConnectionRequest):
    try:
        pid = manager.utilftpget(req.dict())
        return {"process_id": pid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/utilftpgetstatus")
def utilftpgetstatus(req: PIDRequest):
    try:
        status, error = manager.utilftpgetstatus(req.pid)
        res = {"status": status}
        if error:
            res["error"] = error
        return res
    except KeyError:
        raise HTTPException(status_code=404, detail="Process id not found")


@router.post("/utilftpgetlistfiles")
def utilftpgetlistfiles(req: PIDRequest):
    try:
        files = manager.utilftpgetlistfiles(req.pid)
        return {"files": files}
    except KeyError:
        raise HTTPException(status_code=404, detail="Process id not found")


@router.get("/utilftpgetfile/{pid}")
def utilftpgetfile(pid: int, filename: str = Query(...)):
    try:
        file_data = manager.utilftpgetfile(pid, filename)
        return Response(content=file_data, media_type="application/octet-stream",
                       headers={"Content-Disposition": f"attachment; filename={filename}"})
    except KeyError:
        raise HTTPException(status_code=404, detail="Process id not found")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")


@router.post("/utilftpgetdelete")
def utilftpgetdelete(req: PIDRequest):
    try:
        manager.utilftpgetdelete(req.pid)
        return {"deleted": True}
    except KeyError:
        raise HTTPException(status_code=404, detail="Process id not found")


# also include router into the standalone app so /docs on this app works
app.include_router(router)
