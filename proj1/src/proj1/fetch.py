import argparse
import os
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

MAX_PARALLEL_DOWNLOADS = 8


def build_links(years: list[int], months: list[int]) -> list[str]:
    return [
        *[
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
            for year in years
            for month in months
        ],
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
    ]


def fetch_file(link: str, data_dir: str, position: int) -> None:
    filename = os.path.basename(urllib.parse.urlparse(link).path)
    destination = os.path.join(data_dir, filename)
    
    if os.path.exists(destination):
        print(f"Destination for {filename} is full") 
        return
    downloaded = 0

    with tqdm(
        desc=filename,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        position=position,
        leave=True,
    ) as pbar:

        def reporthook(block_num: int, block_size: int, total_size: int):
            nonlocal downloaded

            # Set the total once urllib knows it.
            if total_size > 0 and pbar.total != total_size:
                pbar.total = total_size
                pbar.refresh()

            # urlretrieve gives us the number of blocks received.
            # Clamp to total_size because the last block may be partial.
            current = block_num * block_size

            if total_size > 0:
                current = min(current, total_size)

            # Only update by the amount we haven't already counted.
            delta = current - downloaded

            if delta > 0:
                pbar.update(delta)
                downloaded = current

        try:
            urllib.request.urlretrieve(
                link,
                destination,
                reporthook,
            )
        except urllib.error.HTTPError as e:
            # Months not yet published (e.g. the rest of the current year) 404
            if os.path.exists(destination):
                os.remove(destination)
            pbar.write(f"Skipping {filename}: HTTP {e.code}")
            return

        # Ensure the bar ends exactly at the actual file size.
        actual_size = os.path.getsize(destination)

        if actual_size > downloaded:
            pbar.update(actual_size - downloaded)

        pbar.n = actual_size
        pbar.refresh()


def fetch(data_dir: str, years: list[int], months: list[int]) -> None:
    os.makedirs(data_dir, exist_ok=True)
    links = build_links(years, months)

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_DOWNLOADS) as pool:
        futures = [
            pool.submit(fetch_file, link, data_dir, position)
            for position, link in enumerate(links)
        ]

        # Calling result() propagates exceptions from worker threads.
        for future in futures:
            future.result()


def main() -> None:
    parser = argparse.ArgumentParser(description="Download NYC taxi data.")

    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory for downloaded files (default: data)",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=list(range(2016, 2021)),
        help="Years of yellow taxi trip data to download (default: 2016-2020, as in the assignment)",
    )
    parser.add_argument(
        "--months",
        type=int,
        nargs="+",
        choices=range(1, 13),
        default=list(range(1, 13)),
        metavar="MONTH",
        help="Months of each year to download (default: 1-12)",
    )

    args = parser.parse_args()

    fetch(args.data_dir, args.years, args.months)
