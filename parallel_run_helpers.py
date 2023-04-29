from pathos.multiprocessing import ProcessingPool as Pool
import importlib.util


def load_function(filename, func):
    spec = importlib.util.spec_from_file_location(filename, filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    function = getattr(module, func)
    return function


def run_function(filename, func, cores, input_data, output_fn):
    # Load the function dynamically from the specified module
    function = load_function(filename, func)

    # Use multiprocessing.Pool to map the input data to the function
    with Pool(processes=cores) as pool:
        result = pool.map(function, input_data)

    with open(output_fn, "w") as out:
        for line in result:
            out.write(str(line) + "\n")
