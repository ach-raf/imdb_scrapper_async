import gc
import gzip
import logging
import multiprocessing
import os
import shutil
import time
import urllib.request
import datetime
import re
from imdb_scrapper.lib.imdb import Imdb, ImdbMovie, ImdbSerie
from imdb_scrapper.lib.imdb_id import (
    write_imdb_url,
    get_imdb_urls_dump_path,
    MAX_CHUNK_LENGHT,
    read_imdb_dump,
)
from imdb_scrapper.lib.async_scrapper import (
    process,
    set_up_database,
    slice_list,
    database_excute_command,
)


CURRENT_DIR_PATH = os.path.dirname(os.path.realpath(__file__))

ROOT_DIRECTORY = os.path.normpath(CURRENT_DIR_PATH + os.sep + os.pardir)

DATA_ROOT_DIR = os.path.join(ROOT_DIRECTORY, "data")

ZIP_DIR_PATH = os.path.join(DATA_ROOT_DIR, "title.ratings.tsv.gz")

DATA_OUTPUT_PATH = os.path.join(DATA_ROOT_DIR, "data.tsv")

logging.basicConfig(
    format="%(levelname)s:%(message)s", encoding="utf-8", level=logging.INFO
)

logger = logging.getLogger(__name__)


def download_data(url):
    try:
        with urllib.request.urlopen(url) as file:
            response = file.read()
        return response
    except urllib.error.HTTPError:
        return None
    except urllib.error.URLError:
        return None


def save_file(response, path):
    try:
        with open(path, "wb") as file:
            file.write(response)
        return path
    except IOError:
        return None


def unzip(zip_path, output_path):
    try:
        with gzip.open(zip_path, "rb") as file:
            with open(output_path, "wb") as file_out:
                shutil.copyfileobj(file, file_out)
        return output_path
    except IOError:
        return None


def get_imdb_data_path():
    url = "https://datasets.imdbws.com/title.ratings.tsv.gz"
    response = download_data(url)
    zip_path = save_file(response, ZIP_DIR_PATH)
    data_path = unzip(zip_path, DATA_OUTPUT_PATH)
    if data_path:
        os.remove(zip_path)
        return data_path
    else:
        return None


def filter_list(_list, base_year):
    imdb_base_path = "https://www.imdb.com/title/"
    filtered_list = []
    for _row in iter(_list):
        date = _row[1]
        if "NA" and "Not Rated" and "PG" not in date:
            match date.split("-"):
                case [year, month, day]:
                    media_year = int(year)
                case [year, month]:
                    media_year = int(year)
                case [year] if int():
                    media_year = int(year)
            if media_year >= base_year:
                filtered_list.append(f"{imdb_base_path}{_row[0]}")
    return filtered_list


def get_year_to_update_from(number_of_years):
    """calculate date to update from.

    Args:
        number_of_years ([int]): [number of years back in the past]

    Returns:
        [int]: [current year - number_of_years]
    """
    today = datetime.date.today()
    base_year = today - datetime.timedelta(days=number_of_years * 365)
    return int(datetime.datetime.strptime(str(base_year), "%Y-%m-%d").year)


def list_to_be_updated(number_of_years) -> None:
    def _helper(_table_name):
        query = f""" SELECT imdb_id, release_date from {_table_name}"""
        _rows = database_excute_command(query, "fetch_all")
        base_year = get_year_to_update_from(number_of_years)
        return filter_list(_rows, base_year)

    movies_list = _helper("movie_details")
    series_list = _helper("serie_details")
    _list_to_be_updated = movies_list + series_list
    write_imdb_url(_list_to_be_updated)
    del movies_list
    del series_list
    del _list_to_be_updated
    gc.collect()


if __name__ == "__main__":
    # main()
    # list_to_be_updated(10, "movie_details")
    print(get_imdb_data_path())
