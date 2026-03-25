FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --upgrade pip && pip install -r requirements.txt

ENV HOST=0.0.0.0

CMD ["python", "imdb_etl_mysql_admin_secure.py"]