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
import os.path

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
parser.add_argument('--straw', action="store_true",
                    help="""If present, full_sim_strawman script will be used
                         instead of full_sim script""")
parser.add_argument('--args', nargs=argparse.REMAINDER,
                    help="""Additional arguments beyond this are passed on
                         to the full_sim scripts""")

args = parser.parse_args()

# Prevent multiple script arguments
if args.ara and args.straw:
    raise ValueError("Multiple script options not allowed!")

# Set script and name
if args.ara:
    script_file = "/home/fasig/scalable_radio_array/full_sim_ara.sh"
    descriptive_name = "full_sim_ara_"+args.args[0]
elif args.straw:
    script_file = "/home/fasig/scalable_radio_array/full_sim_strawman.sh"
    descriptive_name = "full_sim_straw_"+args.args[0]
else:
    script_file = "/home/fasig/scalable_radio_array/full_sim.sh"
    descriptive_name = "full_sim_"+args.args[0]

if "-n" in args.args:
    descriptive_name += "_n"+args.args[args.args.index("-n")+1]
else:
    descriptive_name += "_n10"

descriptive_name += "x"+str(args.iterations)


zfill_amount = len(str(args.iterations-1))

output_index = -1
if "-o" in args.args:
    output_index = args.args.index("-o") + 1
    output_name = os.path.basename(args.args[output_index])
    output_dirname = os.path.dirname(args.args[output_index])

logfile_index = -1
if "-l" in args.args:
    logfile_index = args.args.index("-l") + 1
    logfile_name = os.path.basename(args.args[logfile_index])
    # logfile_dirname = os.path.dirname(args.args[logfile_index])

# Declare the error, output, log, and submit directories for Condor Job
error = '/scratch/fasig/pycondor'
output = '/scratch/fasig/pycondor'
log = '/scratch/fasig/pycondor'
submit = '/scratch/fasig/pycondor'

# Setting up a PyCondor Job
job = Job(descriptive_name, script_file,
          error=error, output=output,
          log=log, submit=submit, verbose=2,
#          initialdir=output_dirname,
          extra_lines=["should_transfer_files = YES",
                       "transfer_output_files = output/",
                       "when_to_transfer_output = ON_EXIT_OR_EVICT"])
        #   request_memory="5GB")

# Adding arguments to job
file_remaps = []
for energy in args.energies:
    for i in range(args.iterations):
        if output_index!=-1:
            replaced_name = output_name.replace("ENERGY", energy)
            replaced_name = replaced_name.replace("ITERATION",
                                                  str(i).zfill(zfill_amount))
            args.args[output_index] = os.path.join("output", replaced_name)
            file_remaps.append(replaced_name+'='+os.path.join(output_dirname, replaced_name))
        if logfile_index!=-1:
            o = args.args[output_index]
            replaced_name = logfile_name.replace("OUTPUT", o[:o.rindex(".")])
            replaced_name = replaced_name.replace("ENERGY", energy)
            replaced_name = replaced_name.replace("ITERATION",
                                                  str(i).zfill(zfill_amount))
            args.args[logfile_index] = os.path.join("output", replaced_name)
        job.add_arg(" ".join([energy]+args.args))

# Make sure output files are placed where expected
job.extra_lines.append('transfer_output_remaps = "'+' ; '.join(file_remaps)+'"')

# Write all necessary submit files and submit job to Condor
job.build_submit()
