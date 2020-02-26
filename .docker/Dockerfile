FROM python:3.7.6-slim-buster

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["pytest-watch"]