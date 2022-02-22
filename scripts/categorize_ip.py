import numpy as np
import argparse
import pandas
from collections import defaultdict

from sklearn.ensemble import RandomForestClassifier

def main():

  message = ("This trains on files where the first column is the IP, " +
    "the second column is the label, and the rest are the features.")
  parser = argparse.ArgumentParser(message)
  parser.add_argument('--train', type=str, required=True,
    help="The file with the train features and labels and ip addresses.")
  parser.add_argument('--test', type=str, required=True,
    help="The file with the test features and labels and ip addresses.")

  FLAGS = parser.parse_args()

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
