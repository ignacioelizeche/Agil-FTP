import os
import zipfile
from io import BytesIO
from datetime import datetime
from typing import List

import paramiko
from ftplib import FTP_TLS
import posixpath
from typing import Tuple

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


def upload_to_server(host: str, username: str, password: str, remote_directory: str,
                     files: List[Tuple[str, bytes]], port: int = None, conn_type: str = "sftp") -> List[str]:
    """Upload a list of (relative_path, bytes_content) to the remote server under `remote_directory`.

    Returns list of remote paths uploaded.
    """
    uploaded = []
    # Ensure remote_directory is posix-style
    remote_base = remote_directory or "."

    if conn_type.lower() == "ftps":
        port = port or 990
        ftps = FTP_TLS()
        ftps.connect(host, port, timeout=30)
        ftps.auth()
        ftps.login(username, password)
        ftps.prot_p()

        # Helper to create nested dirs on FTPS
        def ensure_dir(path: str):
            # change to base first
            try:
                ftps.cwd(remote_base)
            except Exception:
                # try to create base
                try:
                    ftps.mkd(remote_base)
                    ftps.cwd(remote_base)
                except Exception:
                    pass
            parts = [p for p in path.split("/") if p]
            for p in parts:
                try:
                    ftps.cwd(p)
                except Exception:
                    try:
                        ftps.mkd(p)
                        ftps.cwd(p)
                    except Exception:
                        # if still failing, raise
                        raise
            # go back to root of base
            ftps.cwd(remote_base)

        for relpath, content in files:
            # normalize to posix
            relposix = posixpath.normpath(relpath).lstrip("/")
            if relposix == "" or relposix.startswith(".."):
                continue
            remote_path = posixpath.join(remote_base, relposix)
            remote_dir = posixpath.dirname(remote_path)
            # Ensure remote_dir exists
            if remote_dir and remote_dir != ".":
                ensure_dir(remote_dir)

            # storbinary expects a file-like object
            from io import BytesIO
            bio = BytesIO(content)
            # Navigate to remote_dir then storbinary with filename
            try:
                ftps.cwd(remote_dir or remote_base)
            except Exception:
                # ensure and retry
                ensure_dir(remote_dir)
                ftps.cwd(remote_dir or remote_base)

            with bio:
                ftps.storbinary(f"STOR {posixpath.basename(remote_path)}", bio)

            uploaded.append(remote_path)

        ftps.quit()

    elif conn_type.lower() == "sftp":
        port = port or 22
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        client = paramiko.SFTPClient.from_transport(transport)

        # ensure remote directory exists (posix)
        def ensure_remote_dirs(path: str):
            parts = [p for p in path.split("/") if p]
            cur = ""
            for p in parts:
                cur = posixpath.join(cur, p) if cur else p
                try:
                    client.stat(cur)
                except IOError:
                    try:
                        client.mkdir(cur)
                    except Exception:
                        # Could be created concurrently; ignore
                        pass

        for relpath, content in files:
            relposix = posixpath.normpath(relpath).lstrip("/")
            if relposix == "" or relposix.startswith(".."):
                continue
            remote_path = posixpath.join(remote_base, relposix)
            remote_dir = posixpath.dirname(remote_path)
            if remote_dir and remote_dir != ".":
                ensure_remote_dirs(remote_dir)

            # write bytes to remote file
            with client.open(remote_path, "wb") as f:
                f.write(content)

            uploaded.append(remote_path)

        client.close()
        transport.close()

    else:
        raise ValueError("conn_type must be 'sftp' or 'ftps'")

    return uploaded
