import gzip
from pathlib import Path

from tqdm import tqdm


def export_to_csv(input_dir: Path, out_file: Path):
    files = sorted(input_dir.glob("*.txt.gz"))

    with open(out_file, "w", encoding="utf-8") as out_f:
        for file in tqdm(files, desc="Files", unit="file"):
            with gzip.open(file, "rt", encoding="utf-8") as f, \
                 tqdm(desc=file.name, unit="lines", leave=False) as pbar:
                for line in f:
                    isbn = line.strip()
                    if isbn:
                        out_f.write(isbn + "\n")
                    pbar.update(1)


def main() -> None:
    downloads_folder = Path.home() / "Downloads"
    isbns_dir = downloads_folder / "isbn"

    out_file = downloads_folder / "isbns.csv"
    export_to_csv(isbns_dir, out_file)


if __name__ == "__main__":
    main()
