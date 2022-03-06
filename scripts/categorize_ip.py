import numpy as np
import argparse
import pandas
from collections import defaultdict
import logging

from sklearn.ensemble import RandomForestClassifier

def main():

  message = ("This trains on files where the first column is the IP, " +
    "the second column is the label, and the rest are the features.")
  parser = argparse.ArgumentParser(message)
  parser.add_argument('--train_features', type=str, nargs='+', required=True,
    help="The file(s) with the train features and labels.")
  parser.add_argument('--train_netflows', type=str, nargs='+', required=True,
    help="The file with the train raw netflow features.")
  parser.add_argument('--test_features', type=str, nargs='+', required=True,
    help="The file(s) with the test features and labels.")
  parser.add_argument('--test_netflows', type=str, nargs='+', required=True,
    help="The file with the test raw netflow features.")
  parser.add_argument('--feature_names', type=str, nargs='+', 
    default=[])
  parser.add_argument('--netflow_names', type=str, nargs='+',
    default=[])

  FLAGS = parser.parse_args()

  train_feature_pandas = []
  for f in FLAGS.train_features:
    logging.debug("Reading training feature file %{0}".format(f))
    train_feature_pandas.append(pandas.read_csv(f))

  train_netflow_pandas = []
  for f in FLAGS.train_netflows:
    logging.debug("Reading training netflow file %{0}".format(f))
    train_netflow_pandas.append(pandas.read_csv(f))

  assert(len(train_feature_pandas) == len(train_netflow_pandas)),(
    "The number of train feature sets should be the same as the number " +
      "of train netflow sets.")


  train = pandas.read_csv(FLAGS.train)
  test  = pandas.read_csv(FLAGS.test)

  X_train   = train.iloc[:,2:]
  y_train   = train.iloc[:,1]
  ips_train = train.iloc[:,0]

  X_test   = test.iloc[:,2:]
  y_test   = test.iloc[:,1]
  ips_test = test.iloc[:,0]

  clf = RandomForestClassifier()
  clf.fit(X_train, y_train)


  test_scores = clf.predict_proba(X_test)
  print("test_scores", test_scores)

  # Create a dictionary of ip to all feature vectors associated
  # with that ip.
  ip2scores = defaultdict(list)   
  for i in range(len(test_scores)):
    ip = ips_test.iloc[i]

    print("ip", ip)
    ip2scores[ip].append(test_scores[i])

  ip2avescore = {}
  for k in ip2scores:
    scorelist = ip2scores[k]
    ave = sum(scorelist) / len(scorelist)
    ip2avescore[k] = ave

  print(ip2avescore)
  mysorted = sorted(ip2avescore.items(), key=lambda item: item[1])
  print(mysorted)

main()
