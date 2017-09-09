"""
  Looks at fanout from one ip to others.

"""

import argparse
import vast 
import operator


def main():
  parser = argparse.ArgumentParser("This computes the fanout from each IP" +
    " to other IPs to see if that is a useful feature.")
  
  parser.add_argument('--inputfile', type=str, required=True,
    help="The netflow file in VAST format")

  FLAGS = parser.parse_args()

  with open(FLAGS.inputfile, "r") as infile:
    
    fanout_dictionary = {}

    for line in infile:
      line = line.strip()
      splitline = line.split(",")

      src_ip  = splitline[vast.VAST_SRC_IP]
      dest_ip = splitline[vast.VAST_DEST_IP]
      
      if src_ip not in fanout_dictionary:
        fanout_dictionary[src_ip] = set()
      fanout_dictionary[src_ip].add(dest_ip)

      #if dest_ip not in fanout_dictionary:
      #  fanout_dictionary[dest_ip] = set()
      #fanout_dictionary[dest_ip].add(src_ip)


    outdegree_dictionary = {}
    for ip in fanout_dictionary:
      outdegree = len(fanout_dictionary[ip])
      if outdegree not in outdegree_dictionary:
        outdegree_dictionary[outdegree] = 0
      outdegree_dictionary[outdegree] += 1

      if len(fanout_dictionary[ip]) > 1:
        print ip, len(fanout_dictionary[ip])

    sorted_outdegree = sorted(outdegree_dictionary.items(), 
                              key=operator.itemgetter(0))
    print sorted_outdegree

    infile.close()

main()


