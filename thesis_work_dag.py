#! /usr/bin/env python
#
# thesis_work_dag.py
# Script for submitting many python scripts to HTCondor in a dagman
#
#
# Ben Hokanson-Fasig
# Created   09/27/19
# Last edit 02/20/20
#


from __future__ import division, print_function

# Standard libraries
import argparse
import os.path

# Custom libraries
from pycondor import Job, Dagman

# Parse command line arguments
parser = argparse.ArgumentParser(description="Script for submitting many "+
                                 "python scripts to HTCondor in a dagman.")
parser.add_argument('script', help="Script to be run")
parser.add_argument('--iterations', type=int, default=1,
                    help="Number of iterations to run (each script with "+
                    "different options is submitted this many times)")
parser.add_argument('--maxjobs', type=int, default=0,
                    help="Maximum number of jobs to submit at once "+
                    "(default no limit)""")
parser.add_argument('-v', '--verbose', action="store_true",
                    help="If present, print all debugging messages from "+
                    "pycondor")
parser.add_argument('--memory',
                    help="Amount of memory to be requested")
parser.add_argument('--disk',
                    help="Amount of disk to be requested")
parser.add_argument('--no_transfer_files', action="store_true",
                    help="If present, don't transfer files back intelligently")
parser.add_argument('--args', nargs=argparse.REMAINDER,
                    help="Additional arguments beyond this are passed on "+
                    "to the script""")

args = parser.parse_args()

# Set script and name
script_file = "/home/fasig/thesis_work/run_python_script.sh"
descriptive_name = os.path.basename(args.script[:-3])+"_"+args.args[0]

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
for i in range(args.iterations):
    transfer_files = []
    file_remaps = []
    if output_index!=-1:
        replaced_name = output_name.replace("ITERATION",
                                            str(i).zfill(zfill_amount))
        if not args.no_transfer_files:
            args.args[output_index] = replaced_name
            transfer_files.append(replaced_name)
            file_remaps.append(replaced_name+'='+
                               os.path.join(output_dirname, replaced_name))
            transfer_lines = [
                "should_transfer_files = YES",
                "transfer_output_files = "+", ".join(transfer_files),
                'transfer_output_remaps = "'+'; '.join(file_remaps)+'"',
                "when_to_transfer_output = ON_EXIT"
            ]
        else:
            args.args[output_index] = os.path.join(output_dirname,
                                                   replaced_name)
            transfer_lines = None
    job = Job((descriptive_name+"_"+str(i).zfill(zfill_amount)
               +"_"+str(args.iterations).zfill(zfill_amount)),
              executable=script_file, output=output, error=error,
              log=log, submit=submit,
              request_memory=args.memory,
              request_disk=args.disk,
              extra_lines=transfer_lines,
              verbose=2 if args.verbose else 0)
    job.add_arg(" ".join([args.script]+args.args))
    dag.add_job(job)


# Write all necessary submit files and submit dagman to Condor
if args.maxjobs>0:
    dag.build_submit(submit_options="-maxjobs "+str(args.maxjobs))
else:
    dag.build_submit()
