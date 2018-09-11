#! /usr/bin/env python
#
# run_full_sim_dagman.py
# Script for submitting many full_sim scripts to HTCondor easily
#
#
# Ben Hokanson-Fasig
# Created   09/10/18
# Last edit 09/10/18
#


from __future__ import division, print_function

# Standard libraries
import argparse
import os.path

# Custom libraries
from pycondor import Job, Dagman

# Parse command line arguments
parser = argparse.ArgumentParser(description="""Script for submitting many
                                 full_sim scripts to HTCondor easily.""")
parser.add_argument('inputs', nargs="+",
                    help="""Input files on which to run (each full_sim script
                         will run on a separate file)""")
parser.add_argument('--maxjobs', type=int, default=0,
                    help="""Maximum number of jobs to submit at once
                         (default no limit)""")
parser.add_argument('-v', '--verbose', action="store_true",
                    help="""If present, print all debugging messages from
                    pycondor""")
parser.add_argument('--args', nargs=argparse.REMAINDER,
                    help="""Additional arguments beyond this are passed on
                         to the full_sim scripts""")

args = parser.parse_args()

# Set script and name
script_file = "/home/fasig/scalable_radio_array/full_sim_osu.sh"
descriptive_name = "full_sim_osu_"+args.args[0]


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

# Setting up the PyCondor Dagman
dag = Dagman(descriptive_name, submit=submit, verbose=2 if args.verbose else 0)

# Adding arguments to jobs
for i, infile in enumerate(args.inputs):
    transfer_files = []
    file_remaps = []
    if output_index!=-1:
        replaced_name = output_name.replace("ITERATION",
                                            str(i+1).zfill(4))
        args.args[output_index] = replaced_name
        transfer_files.append(replaced_name)
        file_remaps.append(replaced_name+'='+
                           os.path.join(output_dirname, replaced_name))
    job = Job(descriptive_name+"_"+str(i+1).zfill(4),
              executable=script_file, output=output, error=error,
              log=log, submit=submit, #request_memory="5GB",
              request_disk="20GB",
              extra_lines=["should_transfer_files = YES",
                           "transfer_output_files = "+", ".join(transfer_files),
                           'transfer_output_remaps = "'+'; '.join(file_remaps)+'"',
                           "when_to_transfer_output = ON_EXIT_OR_EVICT"],
              verbose=2 if args.verbose else 0)
    job.add_arg(" ".join([infile]+args.args))
    dag.add_job(job)

# Write all necessary submit files and submit dagman to Condor
if args.maxjobs>0:
    dag.build_submit(submit_options="-maxjobs "+str(args.maxjobs))
else:
    dag.build_submit()
