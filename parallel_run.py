import argparse
import os
import sys

import pandas as pd
from simple_slurm import Slurm

from parallel_run_helpers import run_function


def get_absolute_path(filename):
    if os.path.isabs(filename):
        # If the filename is already an absolute path, return it unchanged
        return filename
    else:
        # Otherwise, construct an absolute path relative to the cwd
        return os.path.abspath(os.path.join(os.getcwd(), filename))


def parse_args():
    parser = argparse.ArgumentParser(description='Run a function in parallel using slurm on some input csv')
    parser.add_argument('file', type=str, help='Python module to load')
    parser.add_argument('func', type=str, help='Function from that file to invoke')
    parser.add_argument('input', type=str, help='CSV input file')
    parser.add_argument('output', type=str, help='output file name base')
    parser.add_argument('--cores-per-job', type=int, default=1, help='Number of cores to use per job')
    parser.add_argument('--num-jobs', type=int, default=1, help='Number of jobs to run')
    parser.add_argument('--job-name', type=str, help='Name of the job')
    parser.add_argument('--python', type=str, default="python", help="Path to Python to invoke")
    parser.add_argument('slurm_args', nargs=argparse.REMAINDER, help='Additional positional arguments passed onto slurm')
    return parser.parse_args()


args = parse_args()
df_input_csv = pd.read_csv(args.input)
N = len(df_input_csv)
num_jobs = args.num_jobs
cores_per_job = args.cores_per_job
module_fn = get_absolute_path(args.file)
func = args.func
input_fn = get_absolute_path(args.input)
output_fn = get_absolute_path(args.output)

job_name = args.job_name or f"{args.input}_{args.func}"
job_args = ['python', sys.argv[0], module_fn, func, input_fn, output_fn] + ['--cores-per-job', str(cores_per_job), '--num-jobs', str(num_jobs), '--job-name', job_name]
print(job_args)

within_slurm = 'SLURM_JOB_ID' in os.environ

if within_slurm:
    # if program is within slurm, (a) filter the input, and (b) call
    # the input on itself
    task_id = int(os.environ.get('SLURM_ARRAY_TASK_ID'))
    task_count = int(os.environ.get('SLURM_ARRAY_TASK_COUNT'))
    start_ix = int((task_id - 1) * (N / task_count))
    end_ix = int(task_id * (N / task_count))
    df_subset = df_input_csv.iloc[start_ix:end_ix]
    output_fn = f'{args.output}_{task_id}-{task_count}'
    print(f"WITHIN SLURM! Task {task_id}/{task_count}. Dataset of length {len(df_subset)} ({start_ix}:{end_ix})")
    run_function(get_absolute_path(args.file), args.func, args.cores_per_job, [row for index, row in df_subset.iterrows()], get_absolute_path(output_fn))

else:
    ### if program is not invoked by slurm, dispatch slurm on itself
    slurm = Slurm(
            array=range(1, num_jobs+1),
            cpus_per_task=args.cores_per_job,
            job_name=job_name,
        )
    slurm.sbatch(" ".join(job_args))
