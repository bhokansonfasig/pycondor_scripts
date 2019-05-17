#! /usr/bin/env python
#
# pycondor_submit.py
# Script for submitting a basic script and arguments to HTCondor easily
#
#
# Ben Hokanson-Fasig
# Created   02/02/17
# Last edit 05/17/19
#


from __future__ import division, print_function

# Standard libraries
import argparse
import sys

# Custom libraries
from pycondor import Job


# Parse arguments
parser = argparse.ArgumentParser(description=("Submit a basic script and "+
                                              "arguments to HTCondor"))
parser.add_argument('script', help="Script to run on Condor")
parser.add_argument('--submit', default='/scratch/fasig/pycondor',
                    help="Directory to store submit files")
parser.add_argument('--log', default='/scratch/fasig/pycondor',
                    help="Directory to store log files")
parser.add_argument('--output', default='/scratch/fasig/pycondor',
                    help="Directory to store output files")
parser.add_argument('--error', default='/scratch/fasig/pycondor',
                    help="Directory to store error files")
parser.add_argument('--initialdir',
                    help="Initial directory for relative paths")
parser.add_argument('--request_memory',
                    help="Memory request to be included in the submit file")
parser.add_argument('--request_disk',
                    help="Disk request to be included in the submit file")
parser.add_argument('--request_cpus',
                    help="Number of CPUs to request in the submit file")
parser.add_argument('--universe',
                    help="Universe execution environment to be used")
parser.add_argument('--getenv', action='store_true',
                    help="If present, use getenv in the submit file")
parser.add_argument('--script_args', nargs=argparse.REMAINDER,
                    help="Additional arguments beyond this are passed on "+
                    "to the submitted script")
args = parser.parse_args()


# Get script basename
start_index = args.script.rfind("/")+1
try:
    extension_index = args.script.index(".")
except ValueError:
    extension_index = len(args.script)
script_name = args.script[start_index:extension_index]

# Setting up a PyCondor Job
job = Job(name=script_name, executable=args.script,
          error=args.error, output=args.output,
          log=args.log, submit=args.submit, verbose=2,
          request_memory=args.request_memory,
          request_disk=args.request_disk,
          request_cpus=args.request_cpus,
          getenv=args.getenv, universe=args.universe,
          initialdir=args.initialdir,
          extra_lines=["should_transfer_files = YES",
                       "when_to_transfer_output = ON_EXIT"])

# Adding arguments to job
if len(args.script_args)>0:
    job.add_arg(" ".join(args.script_args))

# Write all necessary submit files and submit job to Condor
job.build_submit()
