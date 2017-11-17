#! /usr/bin/env python
#
# process_noise.py
# Script for submitting envelope_processor script to HTCondor easily
#
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
from pycondor import Job


# Get script and options
script_file = "/home/fasig/scalable_radio_array/envelope_processor.sh"
options = sys.argv[1:]

descriptive_name = "process_noise_"
descriptive_name += os.path.basename(sys.argv[1])[:-4].rstrip("_0123456789")

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
