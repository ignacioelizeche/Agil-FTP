import os
import threading
import shutil
import base64
import logging
from typing import Dict, Any, List

from services.sftp_service import download_from_server

logger = logging.getLogger(__name__)


class FTPTaskManager:
    def __init__(self, base_tmp: str = "tmp/ftp_tasks"):
        self._lock = threading.Lock()
        self._next_id = 1
        self._tasks: Dict[int, Dict[str, Any]] = {}
        self.base_tmp = base_tmp
        os.makedirs(self.base_tmp, exist_ok=True)

    def _new_id(self) -> int:
        with self._lock:
            nid = self._next_id
            self._next_id += 1
        return nid

    def utilftpget(self, conn_struct: Dict[str, Any]) -> int:
        """Inicia una tarea de descarga FTP/SFTP en background.

        conn_struct (dict) expected keys:
          - host, username, password, directory
          - download_options: dict with optional keys: filename_startswith (list), from_date (ISO str), port, conn_type
        Returns: process id (sequential int)
        """
        pid = self._new_id()
        task_dir = os.path.join(self.base_tmp, str(pid))
        os.makedirs(task_dir, exist_ok=True)

        task = {
            "id": pid,
            "status": "in_progress",
            "files": [],
            "error": None,
            "dir": task_dir,
        }

        self._tasks[pid] = task

        thread = threading.Thread(target=self._run_download, args=(pid, conn_struct), daemon=True)
        thread.start()
        return pid

    def _run_download(self, pid: int, conn_struct: Dict[str, Any]):
        task = self._tasks.get(pid)
        if not task:
            return

        try:
            # Map expected fields into download_from_server parameters
            host = conn_struct.get("host")
            username = conn_struct.get("username")
            password = conn_struct.get("password")
            directory = conn_struct.get("directory", ".")
            options = conn_struct.get("download_options", {}) or {}
            filename_startswith = options.get("filename_startswith")
            from_date = options.get("from_date", "")
            port = options.get("port")
            conn_type = options.get("conn_type", "sftp")

            # Use existing download helper which writes files into the given download_path
            download_from_server(
                host=host,
                username=username,
                password=password,
                directory=directory,
                download_path=task["dir"],
                filename_startswith=filename_startswith,
                from_date=from_date,
                port=port,
                conn_type=conn_type,
            )

            # List files recovered
            files = os.listdir(task["dir"]) if os.path.isdir(task["dir"]) else []
            task["files"] = files
            task["status"] = "completed"
        except Exception as e:
            logger.exception("Error in FTP task %s", pid)
            task["error"] = str(e)
            task["status"] = "error"

    def utilftpgetstatus(self, pid: int) -> str:
        task = self._tasks.get(pid)
        if not task:
            raise KeyError("Process id not found")
        return task["status"]

    def utilftpgetlistfiles(self, pid: int) -> List[str]:
        task = self._tasks.get(pid)
        if not task:
            raise KeyError("Process id not found")
        return task["files"]

    def utilftpgetfile(self, pid: int, filename: str) -> str:
        task = self._tasks.get(pid)
        if not task:
            raise KeyError("Process id not found")
        file_path = os.path.join(task["dir"], filename)
        if not os.path.isfile(file_path):
            raise FileNotFoundError("File not found in task")
        with open(file_path, "rb") as f:
            data = f.read()
        return base64.b64encode(data).decode("ascii")

    def utilftpgetdelete(self, pid: int) -> None:
        task = self._tasks.pop(pid, None)
        if not task:
            raise KeyError("Process id not found")
        # remove files
        try:
            shutil.rmtree(task["dir"], ignore_errors=True)
        except Exception:
            logger.exception("Could not remove task dir %s", task.get("dir"))


# Shared singleton manager that can be imported by REST app
manager = FTPTaskManager()
