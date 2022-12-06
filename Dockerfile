FROM python:3.8.14-slim

WORKDIR /app
COPY src/ /app/src/

RUN pip install -r requirements.txt

CMD ["python", "src/wrangling_data.py"]
CMD ["python", "src/create_tables.py"]
CMD ["python", "src/etl.py"]
