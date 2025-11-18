"""
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import os
import tempfile

from . import pipeline

app = FastAPI(title="API Process SG→ACD (dev local)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/process")
async def process_file(file: UploadFile = File(...), compte471: str = Form(default=""), compte512: str = Form(default=""), rules_path: str = Form(default="")):
    contents = await file.read()
    try:
        res = pipeline.run_full_pipeline(contents, file.filename, rules_path or None, compte471, compte512)
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    download_name = os.path.basename(res['file_path'])
    return {"preview": res['preview'], "n_rows": res['n_rows'], "download_url": f"/download/{download_name}"}

@app.get("/download/{fname}")
def download_result(fname: str):
    tmp = tempfile.gettempdir()
    path = os.path.join(tmp, fname)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=fname, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
"""