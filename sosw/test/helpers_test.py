__all__ = ['count_rows']


def count_rows(fname):
    i = 0
    with open(fname, 'r') as f:
        for i, _ in enumerate(f):
            pass
    return i + 1
