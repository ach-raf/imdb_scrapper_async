FROM python:3.10

WORKDIR /imdb_scrapper_async

COPY requirements.txt .

COPY . .

RUN pip install -r requirements.txt


CMD [ "python", "main.py"]

