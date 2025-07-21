import sys


notices = []

class Notice:
    def append(self, notice):
        sys.stdout.write(notice)
        if not notice.endswith("\n"):
            sys.stdout.write("\n")
        sys.stdout.flush()
