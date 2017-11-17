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
import sys
import os.path

# Custom libraries
from pycondor import Job, Dagman


# Get script and options
generator_script = "/home/fasig/scalable_radio_array/noise_writer.sh"
processor_script = "/home/fasig/scalable_radio_array/envelope_processor.sh"

n_jobs = int(sys.argv[1])
zero_padding = len(str(n_jobs-1))

basename = os.path.basename(sys.argv[2])

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


# Add arguments to jobs
for i in range(n_jobs):
    filename = sys.argv[2]+"_"+str(i).zfill(zero_padding)
    generator_job.add_arg(filename+' --number 100 --size 10 --rms 25e-6')
    processor_job.add_arg(filename+'*.npz')

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
