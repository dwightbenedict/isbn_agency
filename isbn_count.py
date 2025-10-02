from pathlib import Path
import gzip


def count_lines_in_gz(filepath: Path) -> int:
    with gzip.open(filepath, "rt", encoding="utf-8") as f:
        return sum(1 for _ in f)


def count_all_isbns(isbn_dir: Path) -> int:
    total = 0
    for file in isbn_dir.glob("*.txt.gz"):
        total += count_lines_in_gz(file)
    return total


def main() -> None:
    downloads_folder = Path.home() / "Downloads"
    isbn_dir = downloads_folder / "isbn"
    total = count_all_isbns(isbn_dir)
    print(f"Total valid ISBNs: {total:,}")


if __name__ == "__main__":
    main()