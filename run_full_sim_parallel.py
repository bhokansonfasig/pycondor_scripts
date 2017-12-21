#! /usr/bin/env python
#
# run_full_sim_parallel.py
# Script for submitting many full_sim scripts to HTCondor easily
#
#
# Ben Hokanson-Fasig
# Created   12/21/17
# Last edit 12/21/17
#


from __future__ import division, print_function

# Standard libraries
import sys

# Custom libraries
from pycondor import Job


# Get script and options
script_file = "/home/fasig/scalable_radio_array/full_sim.sh"

energies = ["1e6", "2e6", "5e6",
            "1e7", "2e7", "5e7",
            "1e8", "2e8", "5e8",
            "1e9", "2e9", "5e9",
            "1e10"]

iterations = int(sys.argv[1])
zfill_amount = len(str(iterations))

options = sys.argv[2:]

descriptive_name = "full_sim_"+sys.argv[2]

if "-n" in sys.argv:
    descriptive_name += "_n"+sys.argv[sys.argv.index("-n")+1]
else:
    descriptive_name += "_n10"

descriptive_name += "x"+str(iterations)

output_index = -1
if "-o" in options:
    output_index = options.index("-o") + 1
    output_name = options[output_index]

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
for energy in energies:
    for i in range(iterations):
        if output_index!=-1:
            replaced_name = output_name.replace("ENERGY", energy)
            replaced_name = replaced_name.replace("ITERATION",
                                                  str(i+1).zfill(zfill_amount))
            options[output_index] = replaced_name
        job.add_arg(" ".join([energy]+options))

# Write all necessary submit files and submit job to Condor
job.build_submit()
