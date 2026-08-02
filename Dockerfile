FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt producer/requirements.txt
RUN pip install --no-cache-dir -r producer/requirements.txt

COPY producer ./producer
COPY streaming ./streaming

CMD ["python", "producer/click_generator.py"]
