import os
from typing import Optional

import uvicorn
from databases import Database
from fastapi import FastAPI
from lib.async_scrapper import single_scrape

app = FastAPI()
CURRENT_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
DATABASE_NAME = "imdb.db"
MOVIES_TABLE = "movie_details"
SERIES_TABLE = "serie_details"
DATABASE_LOCATION = os.path.join(CURRENT_DIR_PATH, "database", DATABASE_NAME)
IMDB_DB = Database(f"sqlite:///{DATABASE_LOCATION}")


@app.on_event("startup")
async def database_connect():
    await IMDB_DB.connect()


@app.on_event("shutdown")
async def database_disconnect():
    await IMDB_DB.disconnect()


@app.get("/")
async def fetch_data():
    query = f"SELECT * FROM {MOVIES_TABLE} WHERE voters > 1 ORDER BY voters DESC, score DESC LIMIT 200"
    results = await IMDB_DB.fetch_all(query=query)

    return results


@app.get("/movies")
async def fetch_movies():
    query = f"SELECT * FROM {MOVIES_TABLE} WHERE voters > 10000 and countries NOT IN ('IN%','TURK%','BANGL%') ORDER BY score DESC, voters DESC LIMIT 200"
    movies = await IMDB_DB.fetch_all(query=query)

    return movies


@app.get("/series")
async def fetch_movies():
    query = f"SELECT * FROM {SERIES_TABLE} WHERE voters > 10000 and countries NOT IN ('IN%','TURK%','BANGL%') ORDER BY score DESC, voters DESC LIMIT 200"
    series = await IMDB_DB.fetch_all(query=query)

    return series


@app.get("/api/{imdb_id}")
async def fetch_movies(imdb_id: str):
    query = f'SELECT * FROM {MOVIES_TABLE} WHERE imdb_id like "{imdb_id}"'
    search_result = await IMDB_DB.fetch_all(query=query)
    if not search_result:
        query = f'SELECT * FROM {SERIES_TABLE} WHERE imdb_id like "{imdb_id}"'
        search_result = await IMDB_DB.fetch_all(query=query)
    if not search_result:
        single_scrape(imdb_id)
        query = f'SELECT * FROM {MOVIES_TABLE} WHERE imdb_id like "{imdb_id}"'
        search_result = await IMDB_DB.fetch_all(query=query)
        if not search_result:
            query = f'SELECT * FROM {SERIES_TABLE} WHERE imdb_id like "{imdb_id}"'
            search_result = await IMDB_DB.fetch_all(query=query)
    return search_result


@app.get("/api/search/{title}")
async def fetch_movies(title: str, year: Optional[str] = None):
    if not year:
        query = f'SELECT * FROM {MOVIES_TABLE} WHERE title like "%{title}%" ORDER BY voters DESC, score DESC LIMIT 200'
        search_movies = await IMDB_DB.fetch_all(query=query)
        query = f'SELECT * FROM {SERIES_TABLE} WHERE title like "%{title}%" ORDER BY voters DESC, score DESC LIMIT 200'
        search_series = await IMDB_DB.fetch_all(query=query)
        return search_movies + search_series

    query = f'SELECT * FROM {MOVIES_TABLE} WHERE title like "%{title}%" and strftime("%Y", release_date) like "{year}" ORDER BY voters DESC, score DESC LIMIT 200'
    print(query)
    search_movies = await IMDB_DB.fetch_all(query=query)

    query = f'SELECT * FROM {SERIES_TABLE} WHERE title like "%{title}%" and strftime("%Y", release_date) like "{year}" ORDER BY voters DESC, score DESC LIMIT 200'
    search_series = await IMDB_DB.fetch_all(query=query)
    return search_movies + search_series


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
