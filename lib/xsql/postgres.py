import ctypes
import ctypes.util


def get_command_status(curs):

    print(dir(curs))

    libpq = ctypes.pydll.LoadLibrary(ctypes.util.find_library("pq"))
    libpq.PQcmdStatus.argtypes = [ctypes.c_void_p]
    libpq.PQcmdStatus.restype = ctypes.c_char_p

    return libpq.PQcmdStatus(curs.pgresult_ptr).decode("utf-8")
