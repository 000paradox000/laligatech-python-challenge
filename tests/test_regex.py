import re

INPUT_FILES_DIR = r"/home/aricalso/instances/github.com/000paradox000/000paradox000/laligatech-python-challenge-internal/files/input"
SAMPLE_LOG = rf"{INPUT_FILES_DIR}/sample.log"
SAMPLE_LOG_DUMMY = rf"{INPUT_FILES_DIR}/sample_dummy.log"
EPOCH_REGEX = r"\d+"
HOST_REGEX = r"\S+"


def test_epoch():
    expected_values = [
        "1366815793",
        "1366815793",
        "1366815793",
        "1366815795",
        "1366815795",
        "1366815795",
        "1366815795",
        "1366815811",
        "1366815811",
        "1366815811",
        "",
    ]

    idx = 0
    with open(SAMPLE_LOG, "r") as f:
        for value in f:
            result_value = re.findall(EPOCH_REGEX, value)[0]
            expected_value = expected_values[idx]
            assert expected_value == result_value

            idx += 1


def test_hosts():
    expected_from_host_values = [
        "fromhost",
        "fromhost.com",
        "fromhost.com",
        "fromhost",
        "127.0.0.1",
        "fromhost",
        "fromhost.y.un.f",
        "fromhost",
        "fromhost",
        "fromhost",
        "",
    ]

    expected_to_host_values = [
        "tohost",
        "tohost",
        "127.0.0.1",
        "tohost",
        "tohost",
        "tohost.x.y.u",
        "tohost.x.y.u",
        "tohost",
        "tohost",
        "tohost",
        "",
    ]

    idx = 0
    with open(SAMPLE_LOG, "r") as f:
        for value in f:
            expected_from_host_value = expected_from_host_values[idx]
            expected_to_host_value = expected_to_host_values[idx]

            result_values = re.findall(HOST_REGEX, value)

            result_value = result_values[1]
            assert expected_from_host_value == result_value

            result_value = result_values[2]
            assert expected_to_host_value == result_value

            idx += 1


def test_dummy():
    expected_from_host_values = [
        "fromhost",
        "fromhost",
        "fromhost",
        "fromhost",
        "fromhost",
        "fromhost",
        "fromhost",
        "fromhost",
        "fromhost",
        "fromhost",
        "fromhost",
    ]

    expected_to_host_values = [
        "tohost",
        "tohost",
        "tohost",
        "tohost",
        "tohost",
        "tohost",
        "tohost",
        "tohost",
        "tohost",
        "tohost",
        "tohost",
    ]

    idx = 0
    with open(SAMPLE_LOG_DUMMY, "r") as f:
        for value in f:
            expected_from_host_value = expected_from_host_values[idx]
            expected_to_host_value = expected_to_host_values[idx]

            result_values = re.findall(HOST_REGEX, value)

            result_value = result_values[1]
            assert expected_from_host_value == result_value

            result_value = result_values[2]
            assert expected_to_host_value == result_value

            idx += 1