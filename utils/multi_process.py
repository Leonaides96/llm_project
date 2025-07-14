#%%
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
from joblib import Parallel, delayed
from time import perf_counter

sample_numbers = [10000000, 20000000, 30000000, 40000000]

def timer(func):
    def timeit_wrapper(*args, **kwargs):
        start_time = perf_counter()
        result = func(*args, **kwargs)
        end_time = perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper

def get_square(x):
    return sum(x*x for i in range(x))

@timer
def run_serial():
    return [get_square(x) for x in sample_numbers]

@timer
def run_parallel():
    with ProcessPoolExecutor() as ex:
        res = list(ex.map(get_square, sample_numbers))
    return res


@timer
def run_serial_joblib():
 with Parallel(n_jobs=1, prefer="processes", backend="loky") as parallel:
    with tqdm(total=len(sample_numbers), desc="Squaring the numbers") as pbar:
        result = parallel(delayed(get_square)(x) for x in sample_numbers)
        for _ in result:
            pbar.update(1)
    return result

@timer
def run_parallel_joblib():
 with Parallel(n_jobs=-1, prefer="processes", backend="loky") as parallel:
    with tqdm(total=len(sample_numbers), desc="Squaring the numbers") as pbar:
        resi = parallel(delayed(get_square)(x) for x in sample_numbers)
        for _ in resi:
            pbar.update(1)
    return resi
 
run_serial()
run_serial_joblib()
run_parallel()
run_parallel_joblib()