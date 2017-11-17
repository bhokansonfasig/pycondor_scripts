#! /usr/bin/env python
#
# make_noise.py
# Script for submitting noise_generator script to HTCondor easily
#
#
# Ben Hokanson-Fasig
# Created   11/17/17
# Last edit 11/17/17
#


from __future__ import division, print_function

# Standard libraries
import sys

# Custom libraries
from pycondor import Job


# Get script and options
script_file = "/home/fasig/scalable_radio_array/noise_generator.sh"
options = sys.argv[1:]

descriptive_name = "make_noise_"+sys.argv[1]

# Declare the error, output, log, and submit directories for Condor Job
error = '/data/user/fasig/pycondor'
output = '/data/user/fasig/pycondor'
log = '/data/user/fasig/pycondor'
submit = '/data/user/fasig/pycondor'

# Setting up a PyCondor Job
job = Job(descriptive_name, script_file,
          error=error, output=output,
          log=log, submit=submit, verbose=2)

# Adding arguments to job
if len(options)>0:
    job.add_arg(" ".join(options))

# Write all necessary submit files and submit job to Condor
job.build_submit()
