#! /usr/bin/env python
#
# pycondor_submit.py
# Script for submitting a basic script and arguments to HTCondor easily
#
#
# Ben Hokanson-Fasig
# Created   02/02/17
# Last edit 02/02/17
#


from __future__ import division, print_function

# Standard libraries
import sys

# Custom libraries
from pycondor import Job


# Get script and options
script_file = sys.argv[1]
start_index = script_file.rfind("/")+1
try:
    extension_index = script_file.index(".")
except ValueError:
    extension_index = len(script_file)
script_name = script_file[start_index:extension_index]
options = sys.argv[2:]

# Declare the error, output, log, and submit directories for Condor Job
error = '/scratch/fasig/pycondor'
output = '/scratch/fasig/pycondor'
log = '/scratch/fasig/pycondor'
submit = '/scratch/fasig/pycondor'

# Setting up a PyCondor Job
job = Job(script_name, script_file,
          error=error, output=output,
          log=log, submit=submit, verbose=2,
          initialdir='/home/fasig/pycondor_scripts',
          extra_lines=["should_transfer_files = YES",
                       "when_to_transfer_output = ON_EXIT"])

# Adding arguments to job
if len(options)>0:
    job.add_arg(" ".join(options))

# Write all necessary submit files and submit job to Condor
job.build_submit()
