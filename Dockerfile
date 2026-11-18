FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY library_system ./library_system
COPY data ./data

CMD ["python", "-m", "uvicorn", "library_system.branch:app", "--host", "0.0.0.0", "--port", "8000"]
