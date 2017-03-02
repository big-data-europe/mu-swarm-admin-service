import re


def escape_string(value):
    return "\"{}\"".format(
        re.sub(r'([\\"])', r"\\\1", value)
            .replace("\n", r"\\n")
            .replace("\r", r"\\r")
    )


def escape_datetime(value):
    return '"{}"'.format(value.isoformat())


def escape_boolean(value):
    return "true" if value else "false"
