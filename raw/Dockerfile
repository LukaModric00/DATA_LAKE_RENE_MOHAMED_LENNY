FROM python:3.8-slim

WORKDIR /app

COPY ./file_watcher.py .
COPY ./kafka_producer.py .
COPY ./requirements.txt .
COPY raw_data ./raw_data
COPY ../wait-for-it.sh .

RUN pip install --no-cache-dir -r ./requirements.txt

RUN chmod +x ./wait-for-it.sh

CMD ["/bin/bash", "-c", "./wait-for-it.sh kafka:9092 --timeout=60 --strict -- python file_watcher.py && tail -f /dev/null"]