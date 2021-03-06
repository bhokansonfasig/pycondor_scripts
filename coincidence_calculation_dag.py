#! /usr/bin/env python
#
# coincidence_calculation_dag.py
# Script for submitting many coincidence_calculator scripts to HTCondor in a dagman
#
#
# Ben Hokanson-Fasig
# Created   03/11/18
# Last edit 03/11/18
#


from __future__ import division, print_function

# Standard libraries
import argparse
import os.path

# Custom libraries
from pycondor import Job, Dagman

default_energies = ["1e8", "1e9", "1e10"]

# Parse command line arguments
parser = argparse.ArgumentParser(description="Script for submitting many "+
                                 "detector simulation scripts to HTCondor "+
                                 "in a dagman.")
parser.add_argument('--iterations', type=int, default=1,
                    help="Number of iterations to run (each script with "+
                    "different options is submitted this many times)")
parser.add_argument('--energies', nargs='+', default=default_energies,
                    help="Energies over which to run simulations "+
                    "(defaults to 3 energies from 1e8 to 1e10)")
parser.add_argument('--maxjobs', type=int, default=0,
                    help="Maximum number of jobs to submit at once "+
                    "(default no limit)""")
parser.add_argument('-v', '--verbose', action="store_true",
                    help="If present, print all debugging messages from "+
                    "pycondor")
parser.add_argument('--args', nargs=argparse.REMAINDER,
                    help="Additional arguments beyond this are passed on "+
                    "to the script""")

args = parser.parse_args()

# Set script and name
script_file = "/home/fasig/scalable_radio_array/coincidence_calculator.sh"
descriptive_name = "coincidence_calculation_"+args.args[0]

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

# Declare the error, output, log, and submit directories for Condor job
error = '/scratch/fasig/pycondor'
output = '/scratch/fasig/pycondor'
log = '/scratch/fasig/pycondor'
submit = '/scratch/fasig/pycondor'

# Set up the PyCondor Dagman
dag = Dagman(descriptive_name, submit=submit, verbose=2 if args.verbose else 0)


# Add arguments to jobs
for energy in args.energies:
    for i in range(args.iterations):
        transfer_files = []
        file_remaps = []
        if output_index!=-1:
            replaced_name = output_name.replace("ENERGY", energy)
            replaced_name = replaced_name.replace("ITERATION",
                                                  str(i).zfill(zfill_amount))
            args.args[output_index] = replaced_name
            transfer_files.append(replaced_name)
            file_remaps.append(replaced_name+'='+
                               os.path.join(output_dirname, replaced_name))
        job = Job(descriptive_name+"_"+energy+"_"+str(i).zfill(zfill_amount),
                  executable=script_file, output=output, error=error,
                  log=log, submit=submit, #request_memory="5GB",
                  extra_lines=["should_transfer_files = YES",
                               "transfer_output_files = "+
                               ", ".join(transfer_files),
                               'transfer_output_remaps = "'+
                               '; '.join(file_remaps)+'"',
                               "when_to_transfer_output = ON_EXIT"],
                  verbose=2 if args.verbose else 0)
        job.add_arg(" ".join([energy]+args.args))
        dag.add_job(job)


# Write all necessary submit files and submit dagman to Condor
if args.maxjobs>0:
    dag.build_submit(submit_options="-maxjobs "+str(args.maxjobs))
else:
    dag.build_submit()
