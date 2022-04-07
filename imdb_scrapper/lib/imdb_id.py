import os
import pickle as pkl
import re

MAX_CHUNK_LENGHT = 100  # lenght of the chunk
CURRENT_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
ROOT_DIRECTORY = os.path.normpath(CURRENT_DIR_PATH + os.sep + os.pardir)
IMDB_DATA_PATH = os.path.join(ROOT_DIRECTORY, "data", "data.tsv")
url_DUMP_PATH = os.path.join(ROOT_DIRECTORY, "data", "urls_dump")
url_DUMP_NAME = os.path.join(ROOT_DIRECTORY, "data", "urls_dump", "imdb_urls_dump")
os.makedirs(os.path.dirname(url_DUMP_NAME), exist_ok=True)


def substring_after(_string, _delimiter):
    return _string.partition(_delimiter)[2]


def write_imdb_url_dump(data):
    global MAX_CHUNK_LENGHT
    _dumps_path_index = []

    _dump_chunks = [
        data[i : i + MAX_CHUNK_LENGHT] for i in range(0, len(data), MAX_CHUNK_LENGHT)
    ]
    for index, chunk in enumerate(_dump_chunks):
        _dumps_path_index.append(index)
        with open(f"{url_DUMP_NAME}_{index}", "wb") as file:
            pkl.dump(chunk, file)
    return _dumps_path_index


def read_imdb_dump(_url_dump_path):
    with open(_url_dump_path, "rb") as file:
        return pkl.load(file)


def get_dump_path(_dumps_path_index):
    if _dumps_path_index:
        _dumps_path_index.sort()

        return (os.path.normpath(f"{url_DUMP_NAME}_{i}") for i in _dumps_path_index)
    else:
        return None


def get_imdb_id(_string):
    return re.search("([^\s]+)", _string).group(0)


def get_imdb_urls_dump_path() -> list:
    imdb_base_path = "https://www.imdb.com/title/"

    _dumps_path_index = []
    _print_flag = True
    for _file in os.listdir(url_DUMP_PATH):
        if "imdb_urls_dump" in _file:
            if _print_flag:
                print("imdb_urls_dump found loading..")
                _print_flag = False
            _dumps_path_index.append(int(substring_after(_file, "imdb_urls_dump_")))

    if _dumps_path_index:
        return get_dump_path(_dumps_path_index)

    print("imdb_urls_dump not found, please wait.")
    with open(IMDB_DATA_PATH, "r", encoding="utf") as file:
        # ignore first line in data.tsv
        _first_line = file.readline()
        _lines = file.readlines()

    _imdb_url_list = [f"{imdb_base_path}{get_imdb_id(_line)}" for _line in _lines]

    _dumps_path_index = write_imdb_url_dump(_imdb_url_list)
    print("imdb_urls_dump was created successfully.")
    return get_dump_path(_dumps_path_index)


if __name__ == "__main__":
    print("this is a helper file, to get a list of imdb urls")
    print(get_imdb_urls_dump_path())
