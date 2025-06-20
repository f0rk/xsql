import sys


def write_time(total_time):
    sys.stdout.write("Time: {:.3} ms\n".format(float(total_time) * 1000))
