import time

from test_to_import import five


def row_sum(row):
    time.sleep(3)
    return row.x + row.y + five()
