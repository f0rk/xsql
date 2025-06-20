import sys
from decimal import Decimal


def write_time(total_time):
    formatted_ms = "{:.3f}".format(Decimal(total_time) / Decimal("100000"))

    if "." in formatted_ms:
        l, r = formatted_ms.split(".")
    else:
        l = formatted_ms
        r = "0"

    while len(r) < 3:
        r = r + "0"

    formatted_ms = l + "." + r

    sys.stdout.write("Time: {} ms\n".format(formatted_ms))
