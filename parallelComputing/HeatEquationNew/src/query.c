#!/bin/bash

#PBS -l walltime = 00:02:00,nodes=6:ppn=4
#PBS -N parprog
#PBS -q batch

cd $PBS_O_WORKDIR

mpirun --hostfile $PBS_NODEFILE -np 1 ./run.out