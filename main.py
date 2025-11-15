from fastapi import FastAPI
from sftptools_app import app as sftp_app

app = FastAPI(
    title="Main Dispatcher API",
    docs_url="/",
    openapi_url=None
)

#Apis for the diferent tools
app.mount("/sftptools", sftp_app)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, reload=True)
