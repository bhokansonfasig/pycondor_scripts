#! /usr/bin/env python
#
# calculate_station_triggers.py
# Script for submitting parallel scripts to calculate station triggers
#
# Ben Hokanson-Fasig
# Created   11/28/17
# Last edit 11/28/17
#


from __future__ import division, print_function

# Standard libraries
import argparse
import os
import os.path

# Custom libraries
from pycondor import Job, Dagman


# Parse command line arguments
parser = argparse.ArgumentParser(description="""Script for submitting parallel
                                 scripts to calculate station triggers.""")

parser.add_argument('noise_file_basename',
                        help="basename for noise file sets")
parser.add_argument('outfile',
                    help="output file")
parser.add_argument('-r', '--range', type=float,
                    nargs=2, default=[1e-6, 5e-6],
                    help="time range (default 1e-6 to 5e-6)")
parser.add_argument('-s', '--stations', type=int, default=625,
                    help="number of stations (default 625)")
parser.add_argument('-g', '--geometry', type=int, default=1,
                    help="station geometry code "+
                         "(0 = single string of 4 antennas,"+
                         " 1 = 4 strings of 4 antennas;"+
                         " default 1)")
parser.add_argument('-t', '--threshold', type=float, default=2.5,
                    help="trigger threshold (default 2.5)")
parser.add_argument('--tot', type=float, default=0,
                    help="time over threshold (default 0)")
parser.add_argument('-a', '--antennas_hit', type=int, default=0,
                    help="antennas hit requirement (default per geometry)")

args = parser.parse_args()


basename = os.path.basename(args.noise_file_basename)

# Get script and options
script = "/home/fasig/scalable_radio_array/station_triggers_parallel.sh"

# Declare the error, output, log, and submit directories for Condor Job
error = '/data/user/fasig/pycondor'
output = '/data/user/fasig/pycondor'
log = '/data/user/fasig/pycondor'
submit = '/data/user/fasig/pycondor'

# Setting up PyCondor Jobs
calculator_job = Job("calculate_"+basename, script,
                     error=error, output=output,
                     log=log, submit=submit, verbose=2)
culminator_job = Job("culminate_"+basename, script,
                     error=error, output=output,
                     log=log, submit=submit, verbose=2)


# Add arguments to jobs
dirname = os.path.dirname(args.noise_file_basename)
if dirname=="":
    dirname = "."
file_bases = []
for filename in sorted(os.listdir(dirname)):
    filename = filename[:filename.rindex("_")]
    if filename.startswith(basename) and not(filename in file_bases):
        file_bases.append(os.path.join(dirname, filename))

for basename in file_bases:
    arguments = basename
    arguments += " " + str(args.outfile)
    arguments += " --range " + str(args.range[0]) + " " + str(args.range[1])
    arguments += " --stations " + str(args.stations)
    arguments += " --geometry " + str(args.geometry)
    arguments += " --threshold " + str(args.threshold)
    arguments += " --tot " + str(args.tot)
    arguments += " --antennas_hit " + str(args.antennas_hit)
    calculator_job.add_arg(arguments)

culminator_job.add_arg(args.outfile)

# Create job dependencies
# culminator_job doesn't start until calculator_job has finished
calculator_job.add_child(culminator_job)

# Set up a dagman
dagman = Dagman("full_calculation_"+basename, submit=submit, verbose=2)
# Add jobs to dagman
dagman.add_job(calculator_job)
dagman.add_job(culminator_job)

# Write all necessary submit files and submit job to Condor
dagman.build_submit()
