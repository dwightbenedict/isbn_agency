import gzip
from pathlib import Path


def export_to_csv(isbns_dir: Path, isbns_file: Path) -> int:
    seen = set()
    with open(isbns_file, "w", encoding="utf-8") as out_f:
        for file in sorted(isbns_dir.glob("*.txt.gz")):
            with gzip.open(file, "rt", encoding="utf-8") as f:
                for line in f:
                    isbn = line.strip()
                    if not isbn or isbn in seen:
                        continue
                    seen.add(isbn)
                    out_f.write(isbn + "\n")
    return len(seen)


def main() -> None:
    downloads_folder = Path.home() / "Downloads"
    isbns_dir = downloads_folder / "isbn"

    out_file = downloads_folder / "isbns.csv"
    exported = export_to_csv(isbns_dir, out_file)

    print(f"Export finished → {out_file} ({exported:,} unique ISBNs)")


if __name__ == "__main__":
    main()
