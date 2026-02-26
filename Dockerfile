FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestion.py ./ingestion.py
COPY data/raw/Attraction_Belem.csv ./data/raw/Attraction_Belem.csv
CMD ["python", "ingestion.py"]