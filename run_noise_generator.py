#! /usr/bin/env python
#
# run_noise_generator.py
# Script for submitting many noise_generator and envelope_processor scripts
# to HTCondor easily
#
# Ben Hokanson-Fasig
# Created   11/17/17
# Last edit 11/17/17
#


from __future__ import division, print_function

# Standard libraries
import argparse
import os.path

# Custom libraries
from pycondor import Job, Dagman


# Parse command line arguments
parser = argparse.ArgumentParser(description="""Script for submitting many
                                 noise_generator and envelope_processor scripts
                                 to HTCondor easily.""")
parser.add_argument('output',
                    help="output file base name")
parser.add_argument('-j', '--jobs', type=int, default=1,
                    help="number of jobs to submit (default 1)")
parser.add_argument('-n', '--number', type=int, default=10,
                    help="number of events to generate per job (default 10)")
parser.add_argument('-s', '--size', type=int, default=1000,
                    help="""number of events to write per file
                         (allows intermediate results, default 1000)""")
parser.add_argument('-t', '--time', type=float, default=5e-6,
                    help="""time over which to generate noise waveforms
                         (in seconds, default 5e-6)""")
parser.add_argument('-d', '--dt', type=float, default=1e-10,
                    help="waveform time step (in seconds, default 1e-10)")
parser.add_argument('--rms', type=float, default=1,
                    help="RMS voltage (default 1)")
parser.add_argument('-e', '--envelope', default="envelope_",
                    help="envelope file prefix (default 'envelope_')")
parser.add_argument('-a', '--amplification', type=float, default=1,
                    help="amplification to apply before processing (default 1)")

args = parser.parse_args()

job_zero_padding = len(str(args.jobs-1))
max_file_index = int((args.number-1)/args.size)
file_zero_padding = len(str(max_file_index))
add_file_indices = max_file_index!=0

basename = os.path.basename(args.output)

# Get script and options
generator_script = "/home/fasig/scalable_radio_array/noise_writer.sh"
processor_script = "/home/fasig/scalable_radio_array/envelope_processor.sh"

# Declare the error, output, log, and submit directories for Condor Job
error = '/data/user/fasig/pycondor'
output = '/data/user/fasig/pycondor'
log = '/data/user/fasig/pycondor'
submit = '/data/user/fasig/pycondor'

# Setting up PyCondor Jobs
generator_job = Job("make_noise_"+basename, generator_script,
                    error=error, output=output,
                    log=log, submit=submit, verbose=2)
processor_job = Job("process_noise_"+basename, processor_script,
                    error=error, output=output,
                    log=log, submit=submit, verbose=2)


output_suffixes = [str(i).zfill(file_zero_padding)
                   for i in range(max_file_index+1)]

# Add arguments to jobs
for i in range(args.jobs):
    filename = args.output+"_"+str(i).zfill(job_zero_padding)
    arguments = filename
    arguments += " --number " + str(args.number)
    arguments += " --size " + str(args.size)
    arguments += " --time " + str(args.time)
    arguments += " --dt " + str(args.dt)
    arguments += " --rms " + str(args.rms)
    generator_job.add_arg(arguments)

    if add_file_indices:
        files = [filename+"_"+suffix+".npz" for suffix in output_suffixes]
    else:
        files = [filename+".npz"]
    arguments = " ".join(files)
    arguments += " --output " + str(args.envelope)
    arguments += " --amplification " + str(args.amplification)
    processor_job.add_arg(arguments)

# Create job dependencies
# processor_job doesn't start until generator_job has finished
generator_job.add_child(processor_job)

# Set up a dagman
dagman = Dagman("generate_"+basename, submit=submit, verbose=2)
# Add jobs to dagman
dagman.add_job(generator_job)
dagman.add_job(processor_job)

# Write all necessary submit files and submit job to Condor
dagman.build_submit()
