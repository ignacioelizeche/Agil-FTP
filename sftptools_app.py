from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from services.sftp_service import download_from_server, upload_to_server
from fastapi.responses import Response
from typing import List, Optional
import os
from dotenv import load_dotenv
from fastapi import UploadFile, File, Form
from fastapi.responses import JSONResponse
import zipfile
from io import BytesIO
import smtplib
from email.message import EmailMessage
import ssl
import mimetypes
import base64

load_dotenv()  # carga variables del .env

BASE_DOWNLOAD_PATH = os.getenv("BASE_DOWNLOAD_PATH", "C:/Users/adminlambare/agilsftp")

app = FastAPI(title="SFTP/FTPS Tools API")

class ServerRequest(BaseModel):
    host: str
    directory: str
    destination_folder: str
    username: str
    password: str
    filename_startswith: Optional[List[str]] = None
    from_date: Optional[str] = ""
    port: Optional[int] = None
    conn_type: Optional[str] = "sftp"

class SMTPConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    use_tls: bool

class Attachment(BaseModel):
    filename: str
    content: str  # base64 encoded

class MailData(BaseModel):
    sender: str
    sender_name: Optional[str] = None
    recipient: str
    subject: str
    body: str
    html: bool = False
    attachments: Optional[List[Attachment]] = None

class SendMailRequest(BaseModel):
    smtp: SMTPConfig
    mail: MailData

@app.post("/download")
async def server_copy(request: ServerRequest):
    try:
        download_path = os.path.join(BASE_DOWNLOAD_PATH, os.path.basename(request.destination_folder))
        os.makedirs(download_path, exist_ok=True)

        zip_buffer = download_from_server(
            host=request.host,
            username=request.username,
            password=request.password,
            directory=request.directory,
            download_path=download_path,
            filename_startswith=request.filename_startswith or [],
            from_date=request.from_date,
            port=request.port,
            conn_type=request.conn_type
        )

        headers = {"Content-Disposition": f"attachment; filename={request.destination_folder}_archivos.zip"}
        zip_buffer.seek(0)
        return Response(content=zip_buffer.read(), media_type="application/zip", headers=headers)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload")
async def upload_files(
    host: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    remote_directory: str = Form(...),
    conn_type: Optional[str] = Form("sftp"),
    port: Optional[int] = Form(None),
    extract_zip: Optional[bool] = Form(False),
    files: List[UploadFile] = File(...),
):
    """Receive one or more files and forward them to the remote SFTP/FTPS server.

    Form fields:
    - `host`, `username`, `password`, `remote_directory`, `conn_type` (sftp|ftps), `port`, `extract_zip`
    - `files`: one or more file parts. If `extract_zip` is true and a single zip is provided, its contents are uploaded preserving paths.
    """
    try:
        to_upload = []  # List[Tuple[relative_path, bytes]]

        # If user uploaded one ZIP and requested extraction, extract in-memory
        if extract_zip and len(files) == 1 and files[0].filename.lower().endswith(".zip"):
            content = await files[0].read()
            with zipfile.ZipFile(BytesIO(content)) as z:
                for member in z.namelist():
                    # Normalize and prevent traversal
                    norm = os.path.normpath(member).lstrip("\\/")
                    if norm == "" or norm.startswith(".."):
                        continue
                    if member.endswith("/"):
                        continue
                    data = z.read(member)
                    to_upload.append((norm.replace("\\", "/"), data))
        else:
            for upload in files:
                filename = upload.filename or "uploaded_file"
                norm = os.path.normpath(filename)
                if os.path.isabs(norm) or norm.startswith(".."):
                    raise HTTPException(status_code=400, detail=f"Invalid filename in upload: {filename}")
                data = await upload.read()
                # use posix-style paths for remote
                rel = norm.replace("\\", "/")
                to_upload.append((rel, data))

        # Call service to upload to remote server
        uploaded = upload_to_server(
            host=host,
            username=username,
            password=password,
            remote_directory=remote_directory,
            files=to_upload,
            port=port,
            conn_type=conn_type,
        )

        return JSONResponse(content={"uploaded": uploaded})

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/send-email")
def send_email(data: SendMailRequest):
    try:
        msg = EmailMessage()
        # Construir el header From con nombre si se proporciona
        if data.mail.sender_name:
            msg["From"] = f"{data.mail.sender_name} <{data.mail.sender}>"
        else:
            msg["From"] = data.mail.sender
        msg["To"] = data.mail.recipient
        msg["Subject"] = data.mail.subject

        if data.mail.html:
            msg.add_alternative(data.mail.body, subtype="html")
        else:
            msg.set_content(data.mail.body)

        # Procesar y adjuntar archivos si existen
        if data.mail.attachments:
            for attachment in data.mail.attachments:
                try:
                    # Decodificar base64 con corrección de padding
                    content = attachment.content
                    # Agregar padding si es necesario (base64 debe ser múltiplo de 4)
                    missing_padding = len(content) % 4
                    if missing_padding:
                        content += '=' * (4 - missing_padding)
                    
                    file_data = base64.b64decode(content)
                    
                    # Identificar tipo MIME basado en la extensión del archivo
                    mime_type, _ = mimetypes.guess_type(attachment.filename)
                    if mime_type is None:
                        mime_type = "application/octet-stream"
                    
                    # Separar tipo y subtipo (ej: "image/png" -> "image", "png")
                    maintype, subtype = mime_type.split("/", 1)
                    
                    # Adjuntar archivo
                    msg.add_attachment(
                        file_data,
                        maintype=maintype,
                        subtype=subtype,
                        filename=attachment.filename
                    )
                except Exception as e:
                    raise HTTPException(status_code=400, detail=f"Error procesando archivo {attachment.filename}: {str(e)}")

        context = ssl.create_default_context()
        # Desactivar verificación de certificado para IPs sin coincidencia de dominio
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        # Puerto 465: SMTPS (SSL desde el inicio)
        # Puerto 587: SMTP + STARTTLS
        if data.smtp.port == 465:
            with smtplib.SMTP_SSL(data.smtp.host, data.smtp.port, context=context) as server:
                server.login(data.smtp.user, data.smtp.password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(data.smtp.host, data.smtp.port) as server:
                if data.smtp.use_tls:
                    server.starttls(context=context)

                server.login(data.smtp.user, data.smtp.password)
                server.send_message(msg)

        return {"success": True}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/file-to-base64")
async def file_to_base64(file: UploadFile = File(...)):
    """Convierte un archivo a base64 para usarlo en los adjuntos del email"""
    try:
        # Leer el contenido del archivo
        content = await file.read()
        
        # Convertir a base64
        base64_content = base64.b64encode(content).decode("utf-8")
        
        return {
            "filename": file.filename,
            "content_type": file.content_type,
            "size": len(content),
            "base64": base64_content
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


try:
    from services import ftp_rest as _ftp_rest
    # Include the router so routes appear in the main app's docs under the prefix /ftp
    if hasattr(_ftp_rest, "router"):
        app.include_router(_ftp_rest.router, prefix="/ftp")
    else:
        # fallback to mount the sub-app if router not present
        app.mount("/ftp", _ftp_rest.app)
except Exception:
    # If import fails, keep the original app functional; import errors will surface at runtime.
    pass
