# parallel_run

For quickly running a trivially parallelizable function on an input dataframe in slurm.

Maps across `--num-jobs` jobs on slurm, where each job then uses multiprocessing.Pool on `--cores-per-job` workers.

Note: when using a memory intensive model, like a huggingface transformer, avoid using multiprocessing.Pool. Instead, set --cores-per-job=1, and write the mapped function to delegate using `ray`.

Example usage:
```
cd example
python ../parallel_run.py --cores-per-job 2 --num-jobs 2 --job-name example test_write_output.py row_sum test.csv test-output
```
