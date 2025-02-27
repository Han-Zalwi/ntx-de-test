# Gunakan `python:3.11.4` sebagai base image
FROM python:3.8

WORKDIR /etl
COPY requirements.txt .
COPY etl.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "etl.py"]