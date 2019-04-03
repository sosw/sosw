__all__ = ['line_count']

import subprocess


def line_count(file):
    try:
        # This is the FAST way
        return int(subprocess.check_output(f'wc -l {file}', shell=True).split()[0])
    except:
        # This is in case you are running in some creepy environment without shell access
        i = 0
        with open(file, 'r') as f:
            for i, _ in enumerate(f):
                pass
        return i + 1
