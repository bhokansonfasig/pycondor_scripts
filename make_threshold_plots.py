#! /usr/bin/env python
#
# make_threshold_plots.py
# Script for submitting threshold plot script to HTCondor easily
#
#
# Ben Hokanson-Fasig
# Created   09/21/17
# Last edit 09/21/17
#


from __future__ import division, print_function

# Standard libraries
import sys

# Custom libraries
from pycondor import Job


# Get script and options
script_file = "/home/fasig/scalable_radio_array/threshold_plotter.sh"
options = sys.argv[1:]

# Declare the error, output, log, and submit directories for Condor Job
error = '/data/user/fasig/pycondor'
output = '/data/user/fasig/pycondor'
log = '/data/user/fasig/pycondor'
submit = '/data/user/fasig/pycondor'

# Setting up a PyCondor Job
job = Job(str(options)+" threshold_plotter", script_file,
          error=error, output=output,
          log=log, submit=submit, verbose=2)

# Adding arguments to job
if len(options)>0:
    job.add_arg(" ".join(options))

# Write all necessary submit files and submit job to Condor
job.build_submit()
