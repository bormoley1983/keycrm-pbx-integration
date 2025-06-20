#!/usr/bin/env bash
source venv/bin/activate          # or however you manage venvs
exec uvicorn app.main:app --host 0.0.0.0 --port 5000