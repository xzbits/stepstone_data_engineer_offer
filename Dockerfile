FROM python:3.8.14-slim

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

CMD ["python", "wrangling_data.py"]
CMD ["python", "create_tables.py"]
CMD ["python", "etl.py"]
