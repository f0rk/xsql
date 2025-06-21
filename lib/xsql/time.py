import sys
from decimal import Decimal


def write_time(total_time):
    formatted_ms = "{:.3f}".format(Decimal(total_time) / Decimal("1000000"))

    if "." in formatted_ms:
        left, right = formatted_ms.split(".")
    else:
        left = formatted_ms
        right = "0"

    while len(right) < 3:
        right = right + "0"

    formatted_ms = left + "." + right

    sys.stdout.write("Time: {} ms\n".format(formatted_ms))
