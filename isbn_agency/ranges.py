from pathlib import Path
from dataclasses import dataclass
from typing import Any
import json
from pprint import pprint

from bs4 import BeautifulSoup, Tag


@dataclass
class Range:
    min: str | None
    max: str | None


@dataclass
class AllocationBlock:
    publisher_range: Range
    publication_range: Range

    def is_unallocated(self) -> bool:
        return self.publisher_range.min is None and self.publisher_range.max is None


@dataclass
class RegistrationGroup:
    prefix: str
    group_number: str
    agency: str
    allocation_blocks: list[AllocationBlock]


def read_range_message(filepath: Path) -> BeautifulSoup:
    with open(filepath, "r", encoding="utf-8") as file:
        content = file.read()
    soup = BeautifulSoup(content, "xml")
    return soup


def parse_allocation_block(rule: Tag) -> AllocationBlock:
    allocation_min, allocation_max = rule.Range.text.split("-")
    publisher_length = int(rule.Length.text)

    publisher_min = allocation_min[:publisher_length]
    publisher_max = allocation_max[:publisher_length]
    publication_min = allocation_min[publisher_length:]
    publication_max = allocation_max[publisher_length:]

    allocation_block = AllocationBlock(
        publisher_range=Range(
            min=publisher_min or None,
            max=publisher_max or None
        ),
        publication_range=Range(
            min=publication_min or None,
            max=publication_max or None,
        ),
    )
    return allocation_block


def parse_registration_group(group: Tag) -> RegistrationGroup:
    prefix, group_number = group.Prefix.text.split("-")
    agency = group.Agency.text

    allocation_blocks = [
        parse_allocation_block(rule) for rule in group.Rules.find_all("Rule")
    ]

    return RegistrationGroup(
        prefix=prefix,
        group_number=group_number,
        agency=agency,
        allocation_blocks=allocation_blocks
    )


def format_isbn_directory(registration_groups: list[RegistrationGroup]) -> dict[str, Any]:
    directory: dict[str, Any] = {}

    for group in registration_groups:
        prefix = group.prefix
        code = group.group_number

        if prefix not in directory:
            directory[prefix] = {}

        blocks: list[dict[str, Any]] = []
        for block in group.allocation_blocks:
            blocks.append({
                "publisher_range": {
                    "min": block.publisher_range.min,
                    "max": block.publisher_range.max,
                },
                "publication_range": {
                    "min": block.publication_range.min,
                    "max": block.publication_range.max,
                },
                "is_unallocated": block.is_unallocated(),
            })

        directory[prefix][code] = {
            "agency": group.agency,
            "allocation_blocks": blocks,
        }

    return directory


def save_to_json(directory: dict[str, Any], filepath: Path) -> None:
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(directory, f, indent=4, ensure_ascii=False)


def main() -> None:
    root_dir = Path(__file__).parent.parent
    data_dir = root_dir / "data"
    range_message_file = data_dir / "RangeMessage.xml"
    range_message = read_range_message(range_message_file)

    groups_container = range_message.find("RegistrationGroups")
    registation_groups = [
        parse_registration_group(group) for group in groups_container.find_all("Group")
    ]
    isbn_directory = format_isbn_directory(registation_groups)
    pprint(isbn_directory)

    isbn_directory_file = data_dir / "isbn_directory.json"
    save_to_json(isbn_directory, isbn_directory_file)


if __name__ == "__main__":
    main()