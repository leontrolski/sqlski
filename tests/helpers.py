import re


def sub(s):
    return (
        re.sub(r"[\n \t]+", " ", s)
        .replace(" )", ")")
        .replace("( ", "(")
        .strip()
        .split(" ")
    )
