# Gunakan `python:3.11.4` sebagai base image
FROM python:3.8

WORKDIR /api
COPY requirements.txt .
COPY api.py .

RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 6000

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "6000"]