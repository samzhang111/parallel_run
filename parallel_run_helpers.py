import os
import sys
import importlib.util
from pathlib import Path

from multiprocessing import Pool


def load_function(filename, func):
    p = Path(filename)

    # To import the file, we make sure it is on the path
    sys.path.append(str(p.parent))

    #spec = importlib.util.spec_from_file_location(p.stem, filename)
    #module = importlib.util.module_from_spec(spec)
    #spec.loader.exec_module(module)

    module = __import__(p.stem)
    function = getattr(module, func)
    return function


def run_function(filename, func, cores, input_data, output_fn):
    # Load the function dynamically from the specified module
    function = load_function(filename, func)

    if cores > 1:
        # Use multiprocessing.Pool to map the input data to the function
        with Pool(processes=cores) as pool:
            result = pool.map(function, input_data)
    else:
        result = function(input_data)

    with open(output_fn, "w") as out:
        for line in result:
            out.write(str(line) + "\n")

