from __future__ import print_function, absolute_import, division
import json
from pprint import pprint
from amplpy import AMPL, DataFrame, Environment
from builtins import map, range, object, zip, sorted
import sys
import os
from contextlib import contextmanager
import sys, os
import argparse
import numpy as np


parser = argparse.ArgumentParser(description='Strategy generator using AMPL.')
parser.add_argument('file', help='JSON file with expected statistics.')
parser.add_argument('model', 
                    help='Path to ampl model file (MOD file).')
parser.add_argument('--fix-rationality', '-r', default=0.2, type=float, 
                    help='Probability value between 0 and 1 describing how rational is the attacker.')
parser.add_argument('--var-rationality', '-v', type=float, help='Vary the attackers rationality. Value is expected to be between 0 and 1. The algorithm will produce many outputs by iterating from 0 to 1 by the given value')
parser.add_argument('--fix-honeypots', default=1, type=int, 
                    help='Number of honeypots that the defender can allocate in each router.')
parser.add_argument('--var-honeypots', type=int,
                    help='Algorithm will produce set of strategies by going from 0 to a given number of honeypots.')
parser.add_argument('--output', '-o',
                    help='Filename to save the output json file.')
parser.add_argument('--solver', '-s', default='/home/kori/data/prg/ampl/amplide.linux64/minos',
                    help='Solver that should be used to solve the non-linear problem.')
parser.add_argument('--ampl', '-a', default='/home/kori/data/prg/ampl/amplide.linux64',
                    help='Path to AMPL.')



args = parser.parse_args()

    
def main():

    with open(args.file, 'r') as f:
        data = json.load(f)
    
    var_honeypots = list()
    var_rationality = list()

    if args.var_honeypots is not None:
        var_honeypots = range(args.var_honeypots+1)
    else:
        var_honeypots = [args.fix_honeypots]

    if args.var_rationality is not None:
        var_rationality = list(np.arange(0, 1.00000001, args.var_rationality))
    else:
        var_rationality = [args.fix_honeypots]

    fulloutput = dict()
    fulloutput.update({'data':dict()})


    for date in data.get("data"):
        outputs = list()
        stg = data.get("data").get(date).get("attacker_strategy")
        dist = data.get("data").get(date).get("production_distribution")


        for rat in var_rationality:
            for hp in var_honeypots:
                output = compute_defense(stg, dist, num_of_hp=hp, rationality=rat)
                outputs.append(output)
        fulloutput['data'].update({date:outputs})
        # print('\n*****')

    pprint(fulloutput)
    # Write filtered TODOs to file.
        # with open("filtered_data_file.json", "w") as data_file:
        #     filtered_todos = list(filter(keep, todos))
        #     json.dump(filtered_todos, data_file, indent=2)    
    

@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:  
            yield
        finally:
            sys.stdout = old_stdout




def getAllPorts(att_stg, prod_dist):
    return list(set().union(att_stg.keys(), prod_dist.keys()))
    
def compute_defense(att_stg, prod_dist, num_of_hp=args.fix_honeypots, rationality=args.fix_rationality):
    # production ports and attacker's strategy
    df = DataFrame('P')
    df.setColumn('P', list(att_stg.keys()))
    
    ports = getAllPorts(att_stg, prod_dist)
    att = [att_stg.get(x, 0) for x in ports]
    prod = [prod_dist.get(x, 0) for x in ports]
    df.addColumn('s', prod)
    df.addColumn('p', att)
    
    ampl = AMPL(Environment(args.ampl))
    ampl.setOption('solver', args.solver)
    # ampl.setOption('verbosity', 'terse')
    # Read the model file
    ampl.read(args.model)

    # Assign data to s
    ampl.setData(df, 'P')
    ampl.eval('let L :=  {}; let rat := {};'.format(num_of_hp, rationality))
    
    # Solve the model
    with suppress_stdout():
        ampl.solve()
    reward = ampl.getObjective('reward').value()
    
    hp_stg = ampl.getData('{j in P} h[j]')
    ampl.getVariables
    output = dict()
    output.update({'stg':hp_stg.toDict()})
    output.update({'reward':reward})
    output.update({'rationality':rationality})
    output.update({'num_of_hp':num_of_hp})
    
    ampl.close()
    return output


if __name__ == '__main__':
    main()