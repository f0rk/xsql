import sys


notices = []

class Notice:
    def append(self, notice):
        sys.stdout.write(notice)
        sys.stdout.flush()
