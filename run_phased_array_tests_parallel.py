#! /usr/bin/env python
#
# run_phased_array_tests_parallel.py
# Script for submitting many phased_array_test scripts to HTCondor easily
#
#
# Ben Hokanson-Fasig
# Created   05/02/18
# Last edit 05/02/18
#


from __future__ import division, print_function

# Standard libraries
import argparse

# Custom libraries
from pycondor import Job

default_energies = ["1e6", "2e6", "5e6",
                    "1e7", "2e7", "5e7",
                    "1e8", "2e8", "5e8",
                    "1e9", "2e9", "5e9",
                    "1e10"]

# Parse command line arguments
parser = argparse.ArgumentParser(description="""Script for submitting many
                                 full_sim scripts to HTCondor easily.""")
parser.add_argument('iterations', type=int,
                    help="""Number of iterations to run (each full_sim script
                         with different options submitted this many times)""")
parser.add_argument('--energies', nargs='+', default=default_energies,
                    help="""Energies over which to run simulations
                         (defaults to 13 energies between 1e6 and 1e10)""")
parser.add_argument('--args', nargs=argparse.REMAINDER,
                    help="""Additional arguments beyond this are passed on
                         to the full_sim scripts""")

args = parser.parse_args()

# Set script and name
script_file = "/home/fasig/scalable_radio_array/phased_array_test.sh"
descriptive_name = "phased_array_tests_"+args.args[0]

if "-n" in args.args:
    descriptive_name += "_n"+args.args[args.args.index("-n")+1]
else:
    descriptive_name += "_n10"

descriptive_name += "x"+str(args.iterations)


zfill_amount = len(str(args.iterations-1))

output_index = -1
if "-o" in args.args:
    output_index = args.args.index("-o") + 1
    output_name = args.args[output_index]

logfile_index = -1
if "-l" in args.args:
    logfile_index = args.args.index("-l") + 1
    logfile_name = args.args[logfile_index]

# Declare the error, output, log, and submit directories for Condor Job
error = '/data/user/fasig/pycondor'
output = '/data/user/fasig/pycondor'
log = '/data/user/fasig/pycondor'
submit = '/data/user/fasig/pycondor'

# Setting up a PyCondor Job
job = Job(descriptive_name, script_file,
          error=error, output=output,
          log=log, submit=submit, verbose=2)
        #   request_memory="5GB")

# Adding arguments to job
for energy in args.energies:
    for i in range(args.iterations):
        if output_index!=-1:
            replaced_name = output_name.replace("ENERGY", energy)
            replaced_name = replaced_name.replace("ITERATION",
                                                  str(i).zfill(zfill_amount))
            args.args[output_index] = replaced_name
        if logfile_index!=-1:
            o = args.args[output_index]
            replaced_name = logfile_name.replace("OUTPUT", o[:o.rindex(".")])
            replaced_name = replaced_name.replace("ENERGY", energy)
            replaced_name = replaced_name.replace("ITERATION",
                                                  str(i).zfill(zfill_amount))
            args.args[logfile_index] = replaced_name
        job.add_arg(" ".join([energy]+args.args))

# Write all necessary submit files and submit job to Condor
job.build_submit()
