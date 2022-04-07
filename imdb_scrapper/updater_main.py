import gc
import logging
import multiprocessing
import os
import time
from imdb_scrapper.lib.async_scrapper import process, slice_list
from imdb_scrapper.lib.imdb_id import (
    MAX_CHUNK_LENGHT,
    get_imdb_urls_dump_path,
    read_imdb_dump,
)
from imdb_scrapper.lib.updater import get_imdb_data_path, list_to_be_updated

logging.basicConfig(
    format="%(levelname)s:%(message)s", encoding="utf-8", level=logging.INFO
)

logger = logging.getLogger(__name__)


def main():
    logger.info(f"The program will be processing by chunks of {MAX_CHUNK_LENGHT} item")
    _start_time = time.time()
    logger.info(f"Program started {(time.time() - _start_time)} seconds ---")

    imdb_ids_path = get_imdb_urls_dump_path()
    for _dump_path in imdb_ids_path:
        raw_urls = slice_list(read_imdb_dump(_dump_path))
        process_one = multiprocessing.Process(
            target=process,
            args=(
                raw_urls[0],
                "update",
            ),
        )
        process_two = multiprocessing.Process(
            target=process,
            args=(
                raw_urls[1],
                "update",
            ),
        )
        process_one.start()
        process_two.start()
        process_one.join()
        process_two.join()

        os.remove(_dump_path)
        del raw_urls
        gc.collect()
    logger.info(f"Program ended {(time.time() - _start_time)} seconds ---")


if __name__ == "__main__":
    number_of_years = 1
    list_to_be_updated(number_of_years)
    main()
