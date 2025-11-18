# Prototypage local — backend FastAPI + frontend Next.js

1) Pré-requis système (Ubuntu / Debian)
   - Python 3.10+
   - sudo apt install ghostscript python3-tk

2) Backend (dev)
   cd backend
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   # Placez bdd_criteres.xlsx à côté de backend/app/ ou indiquez son path via le champ rules_path
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

3) Frontend (dev)
   cd frontend
   npm install
   npm run dev
   Ouvrir http://localhost:3000

4) Test curl
   curl -F "file=@/chemin/vers/releve.pdf" -F "compte471=" -F "compte512=" http://localhost:8000/process
