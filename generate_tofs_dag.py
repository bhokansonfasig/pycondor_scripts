#! /usr/bin/env python
#
# generate_tofs_dag.py
# Script for submitting many python scripts to HTCondor in a dagman
#
#
# Ben Hokanson-Fasig
# Created   11/18/20
# Last edit 11/18/20
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
parser.add_argument('radii', nargs='+',
                    help="Radii to submit jobs for")
parser.add_argument('-s', '--split', type=int, default=1,
                    help="Number of subsets in detector. If unspecified, the "+
                    "whole detector is used")
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
tof_script = "/home/fasig/thesis_work/generate_tofs_spherical.py"
descriptive_name = os.path.basename(os.path.splitext(tof_script)[0])
descriptive_name += "_"+args.args[0]

zfill_amount = len(str(max(args.radii)))


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
for r in args.radii:
    transfer_files = []
    file_remaps = []
    transfer_lines = [
        "should_transfer_files = YES",
        "when_to_transfer_output = ON_EXIT"
    ]
    for sub in range(args.split):
        if output_index!=-1:
            replaced_name = output_name.replace("RADIUS",
                                                str(r).zfill(zfill_amount))
            if args.split>1:
                replaced_name = replaced_name.replace("SUBSET",
                                                      str(sub).zfill(len(str(args.split-1))))
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
                transfer_lines = [
                    "should_transfer_files = YES",
                    "when_to_transfer_output = ON_EXIT"
                ]

        job_name = descriptive_name+"_"+str(r).zfill(zfill_amount)
        if args.split>1:
            job_name += "_"+str(sub).zfill(len(str(args.split-1)))

        job = Job(job_name,
                  executable=script_file, output=output, error=error,
                  log=log, submit=submit,
                  request_memory=args.memory,
                  request_disk=args.disk,
                  extra_lines=transfer_lines,
                  verbose=2 if args.verbose else 0)

        job_args = [tof_script] + args.args
        if args.split>1:
            job_args += ["--subset", str(sub)]

        job.add_arg(" ".join([tof_script]+args.args+["--subset", str(sub)]))
        dag.add_job(job)


# Write all necessary submit files and submit dagman to Condor
if args.maxjobs>0:
    dag.build_submit(submit_options="-maxjobs "+str(args.maxjobs))
else:
    dag.build_submit()
