import unittest
import numpy as np
from kmedoids_clustering import KMedoidsClustering


class TestKMedoidsClustering(unittest.TestCase):

    def setUp(self):
        self.kmedoids = KMedoidsClustering('11.txt')

    def test_load_data(self):
        features, labels = self.kmedoids.load_data()
        self.assertIsNotNone(features, "Features should not be None")
        self.assertIsNotNone(labels, "Labels should not be None")
        self.assertEqual(features.shape[1], labels.shape[0], "Features and labels should have the same number of rows")

    def test_invalid_load_data(self):
        kmedoids = KMedoidsClustering('non_existent_file.txt')
        features, labels = kmedoids.load_data()
        self.assertIsNone(features, "Features should be None for an invalid file")
        self.assertIsNone(labels, "Labels should be None for an invalid file")

    def test_cluster_and_analyze(self):
        features, _ = self.kmedoids.load_data()
        predicted_labels_and_medoids = self.kmedoids.cluster_and_analyze(features)
        self.assertIsNotNone(predicted_labels_and_medoids, "Predicted labels and medoids should not be None")
        self.assertTrue(len(predicted_labels_and_medoids) > 0, "Predicted labels and medoids should not be empty")

    def test_visualize_clusters(self):
        features, _ = self.kmedoids.load_data()
        predicted_labels_and_medoids = self.kmedoids.cluster_and_analyze(features)
        # Since visualize_clusters() doesn't return a value, we assume it's working if no exceptions are raised.
        try:
            self.kmedoids.visualize_clusters(features, predicted_labels_and_medoids)
        except Exception as e:
            self.fail(f"visualize_clusters raised an exception: {e}")

    def test_elbow_method(self):
        features, _ = self.kmedoids.load_data()
        # Since elbow_method() doesn't return a value, we assume it's working if no exceptions are raised.
        try:
            self.kmedoids.elbow_method(features)
        except Exception as e:
            self.fail(f"elbow_method raised an exception: {e}")


if __name__ == '__main__':
    unittest.main()
