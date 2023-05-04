import numpy as np
from sklearn_extra.cluster import KMedoids
from sklearn.metrics import silhouette_score
from tabulate import tabulate
from sklearn.metrics import pairwise_distances
import os
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans


class KMedoidsClustering:

    def __init__(self, data_file, k_values=range(3, 6)):
        self.data_file = data_file
        self.k_values = k_values

    def load_data(self):
        try:
            with open(self.data_file, "r") as infile:
                data = np.loadtxt(infile, delimiter=",")
        except FileNotFoundError:
            print(f"File {self.data_file} not found.")
            return None, None
        except Exception as e:
            print(f"Error loading data from file {self.data_file}: {e}")
            return None, None

        data = data[:1000, :]
        features = data[:, 1:]
        labels = data[:, 0]
        return features, labels

    def cluster_and_analyze(self, features):
        if features is None:
            return []

        k_medoids_models = [KMedoids(n_clusters=k, init="random", metric="euclidean") for k in self.k_values]
        predicted_labels_and_medoids = []

        for k_medoids in k_medoids_models:
            try:
                k_medoids.fit(features)
                predicted_labels = k_medoids.labels_
                medoids = k_medoids.cluster_centers_
                predicted_labels_and_medoids.append((predicted_labels, medoids))

                silhouette_coef = silhouette_score(features, predicted_labels)
                inertia = sum(np.min(k_medoids.transform(features), axis=1))
                distances = pairwise_distances(features, medoids)

                threshold = np.mean(distances) + 2 * np.std(distances)
                anomalies = np.where(distances > threshold)
                anomalies_count = len(anomalies)

                print(f"K = {k_medoids.n_clusters}, Silhouette Coefficient: {silhouette_coef}, Inertia: {inertia}")
                print(f"Medoids: {medoids}")
                print(f"Anomalies: {anomalies_count}")
                print()

            except Exception as e:
                print(f"Error clustering and analyzing data: {e}")

        return predicted_labels_and_medoids

    def visualize_clusters(self, features, predicted_labels_and_medoids):
        if features is None or not predicted_labels_and_medoids:
            return

        for i, (predicted_labels, medoids) in enumerate(predicted_labels_and_medoids):
            try:
                plt.figure(i)
                plt.scatter(features[:, 0], features[:, 1], c=predicted_labels)
                plt.scatter(medoids[:, 0], medoids[:, 1], marker='*', s=200, c='#050505')
                plt.title(f'K={self.k_values[i]}')
                plt.show()
            except Exception as e:
                print(f"Error visualizing clusters: {e}")

    def elbow_method(self, features):
        if features is None:
            return

        inertias = []
        for k in self.k_values:
            try:
                kmeans = KMeans(n_clusters=k, init="k-means++", n_init=10, max_iter=300, random_state=42)
                kmeans.fit(features)
                inertias.append(kmeans.inertia_)
            except Exception as e:
                print(f"Error performing elbow method: {e}")

        try:
            plt.plot(self.k_values, inertias, '-o')
            plt.xlabel('Number of clusters (k)')
            plt.ylabel('Inertia')
            plt.show()
        except Exception as e:
            print(f"Error plotting elbow method graph: {e}")

def main():
    os.chdir('C:\\Users\\yamin\\OneDrive\\Desktop\\Features')

    kmedoids = KMedoidsClustering('11.txt')
    features, labels = kmedoids.load_data()
    predicted_labels_and_medoids = kmedoids.cluster_and_analyze(features)
    kmedoids.visualize_clusters(features, predicted_labels_and_medoids)
    kmedoids.elbow_method(features)

if __name__ == "__main__":
    main()