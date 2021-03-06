#! /usr/bin/env python
#
# run_frontend_tests.py
# Script for submitting frontend_tests script to HTCondor easily
#
#
# Ben Hokanson-Fasig
# Created   12/12/17
# Last edit 12/12/17
#


from __future__ import division, print_function

# Standard libraries
import sys

# Custom libraries
from pycondor import Job


# Get script and options
script_file = "/home/fasig/scalable_radio_array/frontend_tests.sh"
options = sys.argv[1:]

descriptive_name = "frontends_"+sys.argv[1]+"_"+sys.argv[2]

if "-n" in sys.argv:
    descriptive_name += "_n"+sys.argv[sys.argv.index("-n")+1]
else:
    descriptive_name += "_n10"

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
