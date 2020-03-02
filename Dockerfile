FROM jupyter/pyspark-notebook

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

CMD [ "python", "challenge.py" ]