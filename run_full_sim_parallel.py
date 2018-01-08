#! /usr/bin/env python
#
# run_full_sim_parallel.py
# Script for submitting many full_sim scripts to HTCondor easily
#
#
# Ben Hokanson-Fasig
# Created   12/21/17
# Last edit 01/08/18
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
parser.add_argument('--ara', action="store_true",
                    help="""If present, full_sim_ara script will be used
                         instead of full_sim script""")
parser.add_argument('--pass', action="store_true",
                    help="""Does nothing. Useful for separating off which
                         arguments should be passed to full_sim scripts""")
parser.add_argument('full_sim_options', nargs=argparse.REMAINDER,
                    help="additional options passed on to full_sim scripts")

args = parser.parse_args()

# Set script and name
if args.ara:
    script_file = "/home/fasig/scalable_radio_array/full_sim_ara.sh"
    descriptive_name = "full_sim_ara_"+args.full_sim_options[0]
else:
    script_file = "/home/fasig/scalable_radio_array/full_sim.sh"
    descriptive_name = "full_sim_"+args.full_sim_options[0]

if "-n" in args.options:
    descriptive_name += "_n"+args.full_sim_options[args.full_sim_options.index("-n")+1]
else:
    descriptive_name += "_n10"

descriptive_name += "x"+str(args.iterations)


zfill_amount = len(str(args.iterations-1))

output_index = -1
if "-o" in args.options:
    output_index = args.full_sim_options.index("-o") + 1
    output_name = args.full_sim_options[output_index]

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
            args.full_sim_options[output_index] = replaced_name
        job.add_arg(" ".join([energy]+args.full_sim_options))

# Write all necessary submit files and submit job to Condor
job.build_submit()
