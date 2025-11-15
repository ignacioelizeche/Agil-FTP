import os
import zipfile
from io import BytesIO
from datetime import datetime
from typing import List

import paramiko
from ftplib import FTP_TLS

def download_from_server(host: str, username: str, password: str, directory: str,
                         download_path: str, filename_startswith: List[str] = None,
                         from_date: str = "", port: int = None, conn_type: str = "sftp") -> BytesIO:
    filename_startswith = filename_startswith or []
    os.makedirs(download_path, exist_ok=True)
    seleccionados = []

    if conn_type.lower() == "ftps":
        port = port or 990
        ftps = FTP_TLS()
        # Igual que tu versión que funcionaba
        ftps.connect(host, port, timeout=30)
        ftps.auth()  # siempre
        ftps.login(username, password)
        ftps.prot_p()
        ftps.cwd(directory)

        archivos = ftps.nlst()

        def get_mod_time(f):
            mdtm = ftps.sendcmd(f"MDTM {f}")
            return datetime.strptime(mdtm[4:], "%Y%m%d%H%M%S")

        download_func = lambda f, path: ftps.retrbinary(f"RETR {f}", open(path, "wb").write)
        close_func = ftps.quit

    elif conn_type.lower() == "sftp":
        port = port or 22
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        client = paramiko.SFTPClient.from_transport(transport)
        archivos = client.listdir(directory)

        def get_mod_time(f):
            attr = client.stat(os.path.join(directory, f))
            return datetime.fromtimestamp(attr.st_mtime)

        download_func = lambda f, path: client.get(os.path.join(directory, f), path)
        close_func = lambda: (client.close(), transport.close())

    else:
        raise ValueError("conn_type debe ser 'sftp' o 'ftps'")

    # Filtrar archivos
    for archivo in archivos:
        if filename_startswith and not any(archivo.startswith(p) for p in filename_startswith):
            continue
        if from_date and get_mod_time(archivo) < datetime.fromisoformat(from_date):
            continue
        seleccionados.append(archivo)

    if not seleccionados:
        close_func()
        raise Exception("No se encontraron archivos con los criterios dados")

    # Descargar archivos
    for archivo in seleccionados:
        local_path = os.path.join(download_path, archivo)
        download_func(archivo, local_path)

    close_func()

    # Crear ZIP en memoria
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for archivo in seleccionados:
            zipf.write(os.path.join(download_path, archivo), arcname=archivo)

    zip_buffer.seek(0)
    return zip_buffer
