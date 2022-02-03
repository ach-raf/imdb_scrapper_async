# https://blog.jonlu.ca/posts/async-python-http

# Importing the required modules
import ast
import asyncio
import cProfile
import gc
import json
import logging
import math
import multiprocessing as multiprocess
import os
import pstats
import re
import sqlite3
import sys
import time

import aiohttp
import async_timeout
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lxml.html import fromstring
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.remote.remote_connection import LOGGER

from data.imdb_id import (MAX_CHUNK_LENGHT, get_imdb_ids_dump_path,
                          read_imdb_dump, write_imdb_id)
from dataclass.imdb import Imdb, ImdbMovie, ImdbSerie

################################################################################
# Initialize connection pool

PARALLEL_REQUESTS = 10

CURRENT_DIR_PATH = os.path.dirname(os.path.realpath(__file__))

PATH_TO_CHROME_DRIVER = os.path.join(
    CURRENT_DIR_PATH, 'chromedriver', 'chromedriver')

DATABASE_LOCATION = os.path.join(CURRENT_DIR_PATH, 'database', 'imdb.db')

LOG_LOCATION = os.path.join(CURRENT_DIR_PATH, 'data', 'imdb_scrapper.log')

# create directories if necessary
os.makedirs(os.path.dirname(DATABASE_LOCATION), exist_ok=True)
os.makedirs(os.path.dirname(LOG_LOCATION), exist_ok=True)

# add it to logging.basicConfig to change output from console to a log file, filename=LOG_LOCATION
logging.basicConfig(format='%(levelname)s:%(message)s',
                    encoding='utf-8', level=logging.INFO)

my_logger = logging.getLogger(__name__)

################################################################################


def get_selenium_soup(_link):
    # chrome_options for selenium
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--disable-crash-reporter")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-in-process-stack-traces")
    chrome_options.add_argument("--disable-logging")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--output=/dev/null")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-features=NetworkService")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    chrome_options.headless = True
    # selenium_service = Service(PATH_TO_CHROME_DRIVER)

    browser = webdriver.Chrome(executable_path=PATH_TO_CHROME_DRIVER,
                               options=chrome_options)
    browser.implicitly_wait(0.1)
    browser.get(_link)
    # sleep to let the page load
    time.sleep(0.5)
    _html = browser.page_source
    _soup = BeautifulSoup(_html.text, 'lxml')
    browser.quit()
    browser.close()
    gc.collect()
    return _soup


def get_html(_link):
    try:
        _html = requests.get(_link)
        if _html.status_code == 404:
            my_logger.warning(f'{_link}, 404 page not found')
            return False
        _soup = BeautifulSoup(_html.text, 'lxml')
        return _soup
    except requests.exceptions.ChunkedEncodingError:
        time.sleep(10)
        # get_selenium_soup(_link)
    except requests.exceptions.ConnectionError:
        return False  # get_selenium_soup(_link)


def get_media_info(_link, _sleep_timer=100):
    soup = get_html(_link)
    if not soup:
        return False
    _script = soup.find('script', type='application/ld+json')
    _script = str(_script).replace(
        '</script>', '').replace('<script type="application/ld+json">', '')
    try:
        media_info = json.loads(_script)
        return soup, media_info
    except json.decoder.JSONDecodeError:
        _sleep_timer += 20
        if _sleep_timer >= 250:
            my_logger.warning('Timeout, get_media_info took too long')
            return False
        my_logger.warning(
            f'get_media_info sleep for {_sleep_timer / 60} min, {_link}')
        time.sleep(_sleep_timer)
        get_html(_link)


def database_excute_command(_command, _fetch_type='none'):
    """use this function to interact with the database

    Args:
        _command (str): the sql command to be excuted
        _fetch_type (str, optional): [either none, fetch_one or fetch_all]. Defaults to 'none'.
    """
    try:
        with sqlite3.connect(DATABASE_LOCATION) as _connection:
            _cursor = _connection.cursor()
            match _fetch_type.lower():
                case 'none':
                    result = _cursor.execute(_command)
                case 'fetch_one':
                    result = _cursor.execute(_command).fetchone()
                case 'fetch_all':
                    result = _cursor.execute(_command).fetchall()
            _connection.commit()
            _cursor.close()
        return result
    except sqlite3.Error as e:
        my_logger.warning(_command)
        my_logger.warning(e)
        return False


def check_table_exists(_table_name):
    _command = f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{_table_name}' '''
    _rows = database_excute_command(_command, 'fetch_one')
    if _rows[0]:
        my_logger.info(f'{_table_name} table found')
        return True
    else:
        my_logger.info(f'{_table_name} table not found')
        return False


def set_up_database():
    if not check_table_exists('movie_details'):
        my_logger.info('creating movie_details table')
        _sql_command = \
            f'''
        CREATE TABLE movie_details (imdb_id TEXT NOT NULL PRIMARY KEY, title TEXT, original_title TEXT, score FLOAT, voters INT, plot TEXT,
        poster TEXT, rated TEXT, genre TEXT, media_type TEXT, release_date TEXT, countries TEXT, actors TEXT, director Text, runtime TEXT) '''
        movie_details_result = database_excute_command(_sql_command)

    if not check_table_exists('serie_details'):
        my_logger.info('creating serie_details table')
        _sql_command = \
            f'''
        CREATE TABLE serie_details (imdb_id TEXT NOT NULL PRIMARY KEY, title TEXT, original_title TEXT, score FLOAT, voters INT, plot TEXT,
        poster TEXT, rated TEXT, genre TEXT, media_type TEXT, release_date TEXT, countries TEXT, actors TEXT, creator TEXT, runtime TEXT, years TEXT, seasons TEXT)'''
        serie_details_result = database_excute_command(_sql_command)


def check_item_exists(_imdb_id):
    sql_command = f"""SELECT count(*)
                    FROM movie_details movie
                    INNER JOIN serie_details serie
                    ON movie.imdb_id = serie.imdb_id
                    WHERE (movie.imdb_id like '{_imdb_id}') OR (serie.imdb_id like '{_imdb_id}')
                    """

    _command = f''' SELECT title from movie_details where imdb_id = "{_imdb_id}" '''
    _row = database_excute_command(_command, 'fetch_one')
    if _row:
        return _row[0]
    else:
        _command = f''' SELECT title from serie_details where imdb_id = "{_imdb_id}" '''
        _row = database_excute_command(_command, 'fetch_one')
        if _row:
            return _row[0]
        else:
            return False


def list_to_string(_list):
    formatted_string = _list[0]
    formatted_string = [formatted_string.join(
        f', {item}') for item in _list[1:]]
    return formatted_string


def get_dataframe(_query):
    connection = sqlite3.connect(DATABASE_LOCATION)
    data = pd.read_sql_query(_query, connection)
    connection.close()
    return data


def clean_text(_text):
    cleaned_text = _text.strip().replace('\n', '')
    cleaned_text = cleaned_text.replace('"', '')
    cleaned_text = cleaned_text.replace(';', '')
    cleaned_text = cleaned_text.replace(':', '')
    cleaned_text = cleaned_text.replace('\xa0', '')
    cleaned_text = cleaned_text.replace('&amp;', '&')
    cleaned_text = cleaned_text.replace('&amp', '&')
    cleaned_text = cleaned_text.replace("""&quot""", '')
    cleaned_text = cleaned_text.replace('&apos;', "\'")
    cleaned_text = cleaned_text.replace('&apos', "\'")
    cleaned_text = cleaned_text.replace('             EN', '')
    cleaned_text = cleaned_text.replace('See full summary»', '').replace(
        "'", '').strip()
    return cleaned_text


def get_imdb_id(_link):
    return re.search('https://www.imdb.com/title/(.{10}?|.{9})', _link).group(1)


def get_countries(_soup):
    clean_countries = []
    _css_selectors = ['section.ipc-page-section:nth-child(28) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(32) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(33) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(34) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(35) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(36) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(37) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(40) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(41) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(44) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(45) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)',
                      'section.ipc-page-section:nth-child(46) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > div:nth-child(2) > ul:nth-child(1)'

                      ]
    for _css_selector in iter(_css_selectors):
        try:
            ul = _soup.select_one(_css_selector)
            items = ul.find_all("li")
            clean_countries = [item.text for item in items]
            if 'Color' in clean_countries or 'Black and White' in clean_countries or '$' in clean_countries[0]:
                clean_countries = ['NA']
                continue
        except AttributeError:
            clean_countries = []
            continue
    if 'NA' in clean_countries or not clean_countries:
        try:
            countries = re.search(
                '(?<=\"countriesOfOrigin\":{\"countries\":)(.*)(?=,\"__typename\":\"CountriesOfOrigin\"},\"detailsExternalLinks\")',
                str(_soup)).group(1)
            countries = ast.literal_eval(countries)
            clean_countries = [country['text'] for country in countries]
        except AttributeError:
            clean_countries = ['NA']
        except SyntaxError:
            countries = re.search(
                '(?<=\"countriesOfOrigin\":{\"countries\":)(.*)(?=,\"__typename\":\"CountryOfOrigin\")',
                str(countries)).group(1)

            countries = str(f'{countries}' + '}]')
            """my_logger.info('#######################################################################')
            my_logger.info('countries', countries)
            my_logger.info('#######################################################################')"""
            try:
                countries = ast.literal_eval(countries)
            except SyntaxError:
                countries = re.search(
                    '(?<=\"countriesOfOrigin\":{\"countries\":)(.*)(?=\])',
                    str(countries)).group(1)
                countries = str(f'{countries}' + ']')
            try:
                countries = ast.literal_eval(str(countries))
                clean_countries = [country['text'] for country in countries]
            except KeyError:
                clean_countries = ['NA']
            except TypeError:
                my_logger.warning('TypeError countries', countries)
                raise Exception('TypeError')
            except SyntaxError:
                my_logger.warning('SyntaxError countries', countries)
                raise Exception('SyntaxError')
    if not clean_countries:
        clean_countries = ['NA']
    return ', '.join(clean_countries)


def get_image_full_size(_image):
    _image = re.search(
        '(https://m\.media-amazon\.com/images/M.*?\.)', _image).group(1)
    return f'{_image}jpg'


def get_title(_soup):
    _title = 'NA'
    _css_selector = '.TitleHeader__TitleText-sc-1wu6n3d-0'
    try:
        _title = _soup.select_one(_css_selector).text
    except AttributeError:
        my_logger.warning(f'get_title AttributeError')
    return clean_text(_title)


def get_score(_media_info):
    try:
        _score = float(_media_info['aggregateRating']['ratingValue'])
    except KeyError:
        _score = -1
    return _score


def get_poster(_media_info):
    try:
        _poster = _media_info['image']
        if 'https' in _poster:
            return get_image_full_size(_poster)
        else:
            return _poster
    except KeyError:
        return 'NA'


def get_plot(_media_info):
    try:
        _plot = clean_text(_media_info['description'])
    except KeyError:
        _plot = 'NA'
    return _plot


def get_director(_soup):

    _css_selectors = ['.PrincipalCredits__PrincipalCreditsPanelWideScreen-sc-hdn81t-0 > ul:nth-child(1) > li:nth-child(1) > div:nth-child(2) > ul:nth-child(1)',
                      '.PrincipalCredits__PrincipalCreditsPanelWideScreen-hdn81t-0 > ul:nth-child(1) > li:nth-child(1) > div:nth-child(2) > ul:nth-child(1)',
                      ]
    for _css_selector in _css_selectors:
        try:
            ul = _soup.select_one(_css_selector)
            li = ul.find_all("li")
            _creators = [item.find('a').text for item in li]
            return ', '.join(_creators)
        except AttributeError:
            _creators = ['NA']
            continue
    return ', '.join(_creators)


def clean_creator(_creator):
    person_creator = []
    for _creator in _creator:
        try:
            if _creator['@type'] == 'Person':
                person_creator.append(_creator["name"])
        except KeyError:
            my_logger.warning('This is an Organization')
    if person_creator:
        return list_to_string(person_creator)
    else:
        return 'This was created by an Organization'


def get_seasons(_soup):
    try:
        _seasons = _soup.select('#browse-episodes-season')
        return int(_seasons[0]['aria-label'].replace(' seasons', ''))
    except IndexError:
        try:
            _seasons = _soup.select_one(
                '.BrowseEpisodes__BrowseLinksContainer-sc-1a626ql-4 > a:nth-child(2) > div:nth-child(1)').text
            _cleaned_season = _seasons.replace(' seasons', '').replace(
                ' season', '').replace(' Seasons', '').replace(' Season', '').strip()
            return int(_cleaned_season)
        except AttributeError:
            return 'NA'


def get_series_runtime(_soup):
    _runtime = 'NA'
    _css_selectors = [
        '.TitleBlockMetaData__MetaDataList-sc-12ein40-0 > li:nth-child(4)', '.TitleBlockMetaData__MetaDataList-sc-12ein40-0 > li:nth-child(3)']
    for _css_selector in _css_selectors:
        try:
            _runtime = _soup.select_one(_css_selector).text
            return _runtime.strip()
        except AttributeError:
            my_logger.warning('get_series_runtime AttributeError')
            continue
    return _runtime.strip()


def _get_actors_helper(_soup, _css_selector):
    ul = _soup.select_one(_css_selector)
    li = ul.find_all("li")
    _actors = [item.find('a').text for item in li]
    return _actors


def get_creator_actor(_soup, is_series=False):
    _creator = []
    _actor = []
    _result = []
    _css_selectors = [
        '.PrincipalCredits__PrincipalCreditsPanelWideScreen-sc-hdn81t-0 > ul:nth-child(1)',
        '.PrincipalCredits__PrincipalCreditsPanelWideScreen-sc-hdn81t-0']
    _current_itteration = {'0': 'Creator', '1': 'Star'}
    _loop_counter = 0
    for _css_selector in _css_selectors:
        try:
            div = _soup.select_one(_css_selector)
            li_div = div.find_all("li")
            for li in li_div:
                if _current_itteration[f'{_loop_counter}'] in li.text:
                    temp_ul = li.find("ul")
                    temp_li = temp_ul.find_all("li")
                    if _loop_counter == 0:
                        _creator = [item.find('a').text for item in temp_li]
                    else:
                        _actor = [item.find('a').text for item in temp_li]
        except AttributeError:
            my_logger.warning('get_actors AttributeError')

        _loop_counter += 1

    if not _creator:
        _creator = ['NA']
    if not _actor:
        _actor = ['NA']

    if is_series:
        return ', '.join(_creator), ', '.join(_actor)
    else:
        return ', '.join(_actor)


def get_series_years(_soup):
    try:
        _year = (_soup.select_one(
            '.TitleBlockMetaData__MetaDataList-sc-12ein40-0 > li:nth-child(2)').text).strip()
        return _year[:len(_year) // 2]
    except AttributeError:
        return 'NA'


def get_genres(_media_info, _soup):
    _css_selectors = ['ul.ipc-metadata-list:nth-child(4) > li:nth-child(1) > div:nth-child(2)', 'ul.ipc-metadata-list:nth-child(4) > li:nth-child(2) > div:nth-child(2)',
                      '.Storyline__StorylineMetaDataList-sc-1b58ttw-1 > li:nth-child(1) > div:nth-child(2) > ul:nth-child(1)']
    _genres = []
    _flag = False
    try:
        return ', '.join(_media_info['genres'])
    except KeyError:
        try:
            for _css_selector in _css_selectors:
                ul = _soup.select_one(_css_selector)
                items = ul.find_all("li")
                for item in items:
                    if len(item.text) < 12:
                        _genres.append(item.text)
                if _genres:
                    return ', '.join(_genres)
        except AttributeError:
            _genres = ['NA']
    if not _genres or 'NA' in _genres:
        try:
            _genres = []
            _div_selector = 'div.ipc-chip-list:nth-child(1)'
            div = _soup.select_one(_div_selector)
            items = div.find_all("a")
            for item in items:
                if len(item.text) < 12:
                    _genres.append(item.text)
            if _genres:
                return ', '.join(_genres)
        except AttributeError:
            _genres = ['NA']
    return ', '.join(_genres)


def get_voters(_media_info, _soup):
    try:
        return int(_media_info['aggregateRating']['ratingCount'])
    except KeyError:
        try:
            _div = _soup.select_one(
                'ul.ipc-metadata-list:nth-child(4) > li:nth-child(2) > div:nth-child(2)')
            return int(_div.text)
        except AttributeError:
            return 'NA'
        except ValueError:
            return 'NA'


def get_release_date(_media_info, _soup):
    _release_date = 'NA'
    try:
        _release_date = _media_info['datePublished']
    except KeyError:
        try:
            _div = _soup.select_one(
                '.TitleBlockMetaData__MetaDataList-sc-12ein40-0 > li:nth-child(1) > a:nth-child(1)')
            _release_date = _div.text
        except AttributeError:
            return _release_date
    return _release_date


def get_rated(_media_info, _soup):
    _rated = 'NA'
    try:
        _rated = _media_info['contentRating']
    except KeyError:
        try:
            _div = _soup.select_one(
                'ul.ipc-inline-list--show-dividers:nth-child(2) > li:nth-child(3) > a:nth-child(1)')
            _rated = _div.text
        except AttributeError:
            return _rated
    return _rated


def get_details(_media_data):
    try:
        imdb_id, soup, media_info = _media_data[0], _media_data[1], _media_data[2]
    except TypeError:
        return False
    if not media_info:
        return False
    media_type = media_info['@type']
    if 'TVEpisode' in media_type:
        my_logger.info(f'{imdb_id}: {media_type} Skipped')
        return False
    title = get_title(soup)
    original_title = clean_text(media_info['name'])
    voters = get_voters(media_info, soup)
    rated = get_rated(media_info, soup)
    release_date = get_release_date(media_info, soup)
    poster = get_poster(media_info)
    countries = get_countries(soup)
    score = get_score(media_info)
    plot = get_plot(media_info)
    genre = get_genres(media_info, soup)
    match media_type:
        case 'TVSeries':
            media_type = 'TV Series'
            creator, actors = get_creator_actor(soup, True)
            seasons = get_seasons(soup)
            runtime = get_series_runtime(soup)
            years = get_series_years(soup)
            """try:
                creator = clean_creator(media_info['creators'])
            except KeyError:
                creator = get_creators(soup)"""
            if release_date == 'NA':
                if years != 'NA':
                    release_date = years.split('-')[0]

            return ImdbSerie(imdb_id, title, original_title, score, voters, plot, poster,
                             rated, genre, media_type, release_date, countries, actors, creator, runtime, years,
                             seasons)

        case 'Movie':
            actors = get_creator_actor(soup)
            try:
                director = clean_creator(media_info['directors'])
            except KeyError:
                director = get_director(soup)
            try:
                runtime = media_info['duration'].replace(
                    'PT', '').replace('H', 'h').replace('M', 'm').lower()
            except KeyError:
                runtime = 'NA'
            return ImdbMovie(imdb_id, title, original_title, score, voters, plot,
                             poster, rated, genre, media_type, release_date, countries, actors, director, runtime)

        case _:
            return False


def add_to_database(_media: Imdb):
    _insert_command = _media.insertion_command()
    return database_excute_command(_insert_command)


def get_imdb_id(_link):
    return re.search('https://www.imdb.com/title/(.{10}?|.{9})', _link).group(1)


def build_urls_list(_imdb_ids):
    imdb_base_path = 'https://www.imdb.com/title/'
    return [f'{imdb_base_path}{imdb_id}' for imdb_id in _imdb_ids]


def get_proxies():
    # website to get free proxies
    url = 'https://free-proxy-list.net/'

    response = requests.get(url)

    parser = fromstring(response.text)
    # using a set to avoid duplicate IP entries.
    proxies = set()

    for i in parser.xpath('//tbody/tr')[:10]:

        # to check if the corresponding IP is of type HTTPS
        if i.xpath('.//td[7][contains(text(),"yes")]'):

            # Grabbing IP and corresponding PORT
            proxy = ":".join([i.xpath('.//td[1]/text()')[0],
                              i.xpath('.//td[2]/text()')[0]])

            proxies.add(proxy)
    return proxies


async def gather_with_concurrency(urls, _parallel_requests):
    conn = aiohttp.TCPConnector(limit_per_host=200, limit=0, ttl_dns_cache=300)
    headers = {
        'user-agent': 'Mozilla/5.0 (Linux; Android 7.0; SM-G892A Build/NRD90M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/60.0.3112.107 Mobile Safari/537.36'}
    semaphore = asyncio.Semaphore(_parallel_requests)
    session = aiohttp.ClientSession(connector=conn, trust_env=True)
    results = []
    #_proxy = next(proxyPool)
    # print(f'{_proxy=}')

    # heres the logic for the generator
    async def get(url):
        async with semaphore:
            with async_timeout.timeout(20):
                async with session.get(url, ssl=False, headers=headers) as response:
                    obj = BeautifulSoup(await response.read(), 'lxml')
                    results.append(
                        (get_imdb_id(url), obj))
    await asyncio.gather(*(get(url) for url in urls))
    await session.close()
    await conn.close()
    return results


def build_info(_results):
    _build_info = []
    for result in _results:
        imdb_id = result[0]
        soup = result[1]
        if result:
            _script = soup.find('script', type='application/ld+json')
            _script = str(_script).replace(
                '</script>', '').replace('<script type="application/ld+json">', '')
            try:
                media_info = json.loads(_script)
                _build_info.append((imdb_id, soup, media_info))
            except json.decoder.JSONDecodeError:
                print('Timeout, get_media_info took too long')

    return _build_info


def get_media_data(urls):

    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(
        gather_with_concurrency(urls, PARALLEL_REQUESTS))

    # loop.close()
    return iter(build_info(results))


def get_item(_media_info):
    details = get_details(_media_info)
    if details:
        insertion_details = add_to_database(details)
    if insertion_details:
        my_logger.info(f'{details.title} Added to database')


def process(raw_urls):

    urls = (url for url in raw_urls if not check_item_exists(
        get_imdb_id(url)))
    if not urls:
        return None
    start_media = time.time()
    my_logger.info(
        f"start_media {(time.time() - start_media)} seconds ---")
    _media_infos = get_media_data(urls)

    del urls
    gc.collect()

    my_logger.info(f"end media {(time.time() - start_media)} seconds ---")
    for index,  _media_info in enumerate(_media_infos):
        imdb_id = _media_info[0]
        my_logger.info(f'Processing: {index+1}/{imdb_id}')
        start_time = time.time()

        # time = timeit(f'get_item({imdb_id}, {_media_infos})',
        # setup = 'from __main__ import get_item')
        get_item(_media_info)
        # my_logger.info(time)

        my_logger.info(f"--- {(time.time() - start_time)} seconds ---")
    my_logger.info('sleep for 2 seconds')
    #del urls
    del _media_infos
    gc.collect()
    time.sleep(2)


def slice_list(_list):
    half = math.floor(len(_list)/2)
    return [_list[:half], _list[half:]]


def get_sliced_imdb_ids_path():
    # to rotate through the list of IPs
    proxies = get_proxies()
    #proxyPool = cycle(proxies)
    imdb_ids_path = get_imdb_ids_dump_path()[:5]
    sliced_imdb_ids_path = slice_list(imdb_ids_path)
    return sliced_imdb_ids_path


def main():
    my_logger.info(
        f'The program will be processing by chunks of {MAX_CHUNK_LENGHT} item')
    set_up_database()
    _start_time = time.time()
    my_logger.info(
        f"Program started {(time.time() - _start_time)} seconds ---")

    imdb_ids_path = get_imdb_ids_dump_path()
    for _dump_path in imdb_ids_path:
        raw_urls = slice_list(read_imdb_dump(_dump_path))
        process_one = multiprocess.Process(
            target=process, args=(raw_urls[0],))
        process_two = multiprocess.Process(
            target=process, args=(raw_urls[1],))
        process_one.start()
        process_two.start()
        process_one.join()
        process_two.join()
        os.remove(_dump_path)
        del raw_urls
        gc.collect()
    my_logger.info(f"Program ended {(time.time() - _start_time)} seconds ---")


if __name__ == '__main__':
    with cProfile.Profile() as profile:
        main()
    stats = pstats.Stats(profile)
    stats.sort_stats(pstats.SortKey.TIME)
    # stats.print_stats()
    stats.dump_stats(filename='scrapper_profil_async_6.prof')
