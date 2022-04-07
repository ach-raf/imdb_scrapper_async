# add it to logging.basicConfig to change output from console to a log file, filename=LOG_LOCATION
import gc
import logging
import multiprocessing
import os
import time

from imdb_scrapper.lib.async_scrapper import (
    process,
    set_up_database,
    slice_list,
    clean_urls,
)
from imdb_scrapper.lib.imdb_id import (
    MAX_CHUNK_LENGHT,
    get_imdb_urls_dump_path,
    read_imdb_dump,
)


logging.basicConfig(
    format="%(levelname)s:%(message)s", encoding="utf-8", level=logging.INFO
)

logger = logging.getLogger(__name__)


def main():
    logger.info(f"The program will be processing by chunks of {MAX_CHUNK_LENGHT} item")
    set_up_database()
    _start_time = time.time()
    logger.info(f"Program started {(time.time() - _start_time)} seconds ---")

    imdb_ids_path = get_imdb_urls_dump_path()
    for _dump_path in imdb_ids_path:
        raw_urls = read_imdb_dump(_dump_path)
        urls = slice_list(clean_urls(raw_urls))
        if not urls:
            continue
        process_one = multiprocessing.Process(
            target=process,
            args=(
                urls[0],
                "add",
            ),
        )
        process_two = multiprocessing.Process(
            target=process,
            args=(
                urls[1],
                "add",
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
    main()
