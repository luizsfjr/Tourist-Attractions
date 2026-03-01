FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main_pandas.py ./main_pandas.py
COPY ingestion.py ./ingestion.py
COPY transform.py ./transform.py
COPY project_config.py ./project_config.py
COPY data/raw/Attraction_Belem.csv ./data/raw/Attraction_Belem.csv

CMD ["python", "main_pandas.py"]
