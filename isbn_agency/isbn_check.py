from typing import Any


def is_valid_isbn(isbn: str, directory: dict[str, Any]) -> bool:
    prefix, body = isbn[:3], isbn[3:-1]
    if prefix not in directory:
        return False

    for group_num, blocks in directory[prefix]:
        if body.startswith(group_num):
            pub_block = body[len(group_num):]
            for pub_len, pub_min, pub_max, pubn_len, pubn_min, pubn_max in blocks:
                publisher = pub_block[:pub_len]
                publication = pub_block[pub_len:]
                if pub_min <= publisher <= pub_max and (not pubn_min or pubn_min <= publication <= pubn_max):
                    return True
            return False
    return False

