import re
from pathlib import Path

INPUT_FILES_DIR = Path(__file__).resolve().parent / "files"
COMPLEX_LOG = INPUT_FILES_DIR / "complex.log"
SIMPLE_LOG = INPUT_FILES_DIR / "simple.log"
HOST_REGEX = r"\S+"


def test_complex_log():
    with open(COMPLEX_LOG, "r") as f:
        for value in f:
            parts = value.strip().replace("\t", "").split(" ")
            unix_time, from_host, to_host = [item for item in parts if item]

            result_values = re.findall(HOST_REGEX, value)

            result_value = result_values[1]
            assert from_host == result_value

            result_value = result_values[2]
            assert to_host == result_value


def test_dummy():
    with open(SIMPLE_LOG, "r") as f:
        for value in f:
            parts = value.strip().replace("\t", "").split(" ")
            unix_time, from_host, to_host = [item for item in parts if item]

            result_values = re.findall(HOST_REGEX, value)

            result_value = result_values[1]
            assert from_host == result_value

            result_value = result_values[2]
            assert to_host == result_value
