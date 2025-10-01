from typing import Any
from pathlib import Path
import json
import gzip
import multiprocessing as mp

import numpy as np
from tqdm import tqdm

from isbn_agency.isbn_check import is_valid_isbn


isbn_directory_cache = None



def compute_check_digits(bases: np.ndarray) -> np.ndarray:
    """
    bases: np.ndarray of strings, each 12 digits (prefix+body).
    Returns: np.ndarray of check digit strings.
    """
    digits = np.frombuffer("".join(bases.tolist()).encode(), dtype=np.uint8) - 48
    digits = digits.reshape(len(bases), 12)

    weights = np.tile([1, 3], 6)  # [1,3,1,3,...] length 12
    s = (digits * weights).sum(axis=1)

    return ((10 - (s % 10)) % 10).astype(str)


def enumerate_isbns(prefix: str, start: int, stop: int, chunk_size: int = 100_000):
    for chunk_start in range(start, stop, chunk_size):
        chunk_end = min(chunk_start + chunk_size, stop)
        numbers = np.arange(chunk_start, chunk_end, dtype=np.int64)

        body = np.char.zfill(numbers.astype(str), 9)   # pad to 9 digits
        bases = np.char.add(prefix, body)              # 12-digit bases
        checks = compute_check_digits(bases)
        isbns = np.char.add(bases, checks)

        yield isbns


def read_isbn_directory(filepath: Path) -> dict[str, Any]:
    with filepath.open("r", encoding="utf-8") as f:
        return json.load(f)


def optimize_directory(directory: dict[str, Any]) -> dict[str, Any]:
    restructured: dict[str, Any] = {}
    for prefix, groups in directory.items():
        new_groups = []
        for group_num in sorted(groups.keys(), key=len, reverse=True):
            group = groups[group_num]
            blocks = []
            for block in group["allocation_blocks"]:
                if block["is_unallocated"]:
                    continue
                pub_min = block["publisher_range"]["min"]
                pub_max = block["publisher_range"]["max"]
                pubn_min = block["publication_range"]["min"]
                pubn_max = block["publication_range"]["max"]
                blocks.append((
                    len(pub_min), pub_min, pub_max,
                    len(pubn_min) if pubn_min else 0, pubn_min, pubn_max
                ))
            new_groups.append((group_num, blocks))
        restructured[prefix] = new_groups
    return restructured


def process_range(
    prefix: str,
    start: int,
    stop: int,
    out_file: Path,
    chunk_size: int,
    flush_size: int
) -> None:
    buffer: list[str] = []
    with gzip.open(out_file, "wt", encoding="utf-8", compresslevel=5) as f:
        for isbns in tqdm(
                enumerate_isbns(prefix, start, stop, chunk_size),
                total=((stop - start) // chunk_size),
                desc=f"{prefix}-{mp.current_process().name}"
        ):
            for isbn in isbns:
                if is_valid_isbn(isbn, isbn_directory_cache):
                    buffer.append(isbn)
                if len(buffer) >= flush_size:
                    f.write("\n".join(buffer) + "\n")
                    buffer.clear()
        if buffer:
            f.write("\n".join(buffer) + "\n")
            buffer.clear()


def init_worker(directory: dict[str, Any]):
    global isbn_directory_cache
    isbn_directory_cache = directory


def main() -> None:
    downloads_folder = Path.home() / "Downloads"
    isbn_folder = downloads_folder / "isbn"

    isbn_directory_file = Path("data/isbn_directory.json")
    isbn_directory = read_isbn_directory(isbn_directory_file)

    optimized_directory = optimize_directory(isbn_directory)

    prefixes = ["978", "979"]
    space = 1_000_000_000  # 9-digit bodies
    chunk_size = 1_000_000
    flush_size = 100_000

    workers = 8

    for prefix in prefixes:
        step = space // workers
        tasks = []
        for i in range(workers):
            start = i * step
            stop = (i + 1) * step if i < workers - 1 else space
            out_file = isbn_folder / f"{prefix}_{i}.txt.gz"
            tasks.append(
                (prefix, start, stop, out_file, chunk_size, flush_size)
            )

        with mp.Pool(workers, initializer=init_worker, initargs=(optimized_directory,)) as pool:
            pool.starmap(process_range, tasks)



if __name__ == "__main__":
    main()