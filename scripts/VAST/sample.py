"""
  Samples the majority class and minority class according to input 
  probabilities. 
"""

import argparse
import vast
import random

def main():

  parser = argparse.ArgumentParser("Samples the majority class (with a label" +
    " of 0) and minority class (with label 1) according to specified " +
    "probabilities")
  
  parser.add_argument("--inputfile", type=str, required=True,
    help="The netflow file to read from (VAST format)")

  parser.add_argument("--outputfile", type=str, required=True,
    help="Where the new netflow file will be written.")

  parser.add_argument("--prob0", type=float, default=0.1,
    help="Probability of including majority class example")

  parser.add_argument("--prob1", type=float, default=0.1,
    help="Probability of including majority class example")

  FLAGS = parser.parse_args()

  with open(FLAGS.inputfile, "r") as infile:
    with open(FLAGS.outputfile, "w") as outfile:

      for line in infile:
        line = line.strip()
        splitline = line.split(",")

        label = splitline[vast.VAST_LABEL]

        if label == "0":
          p = random.random()
          if p < FLAGS.prob0:
            outfile.write(line + "\n")
        else:
          p = random.random()
          if p < FLAGS.prob1:
            outfile.write(line + "\n")
     
    outfile.close()
  infile.close()

main()
