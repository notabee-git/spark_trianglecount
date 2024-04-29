from pyspark import SparkContext
import sys
import numpy as np

# ABHIRAM VELADINENI - 21EE10002

def read_input_file(input_file_path):
    """
    Read the input file and return RDD of edges.
    """
    sc = SparkContext("local", "Triangle Count")
    text_rdd = sc.textFile(input_file_path)
    return text_rdd

def extract_edges(text_rdd):
    """
    Extract edges from RDD and return a list of edges.
    """
    return text_rdd.map(lambda line: line.split()).collect()

def create_adj_list(edge_list):
    """
    Create an adjacency list from the edge list.
    """
    adj_list = {}
    for edge in edge_list:
        source = int(edge[0])
        destination = int(edge[1])
        adj_list.setdefault(source, []).append(destination)
        adj_list.setdefault(destination, []).append(source)
    return adj_list

def find_nodes_set(edge_list):
    """
    Find unique nodes from the edge list.
    """
    nodes_set = set()
    for edge in edge_list:
        nodes_set.add(edge[0])
        nodes_set.add(edge[1])
    return nodes_set

def count_num_edges(edge_list):
    """
    Count the number of edges in the edge list.
    """
    return len(edge_list)

def count_heavy_hitters(adj_list, threshold):
    """
    Count the number of heavy hitter nodes in the adjacency list.
    """
    return len([node for node, neighbors in adj_list.items() if len(neighbors) >= threshold])

def count_triangles(nodes_array, adj_list):
    """
    Count the number of triangles in the graph.
    """
    triangle_count = 0
    for node, neighbors in adj_list.items():
        for i in range(len(neighbors)-1):
            for j in range(i + 1, len(neighbors)):
                neighbor1 = neighbors[i]
                neighbor2 = neighbors[j]
                if neighbor2 in adj_list[neighbor1]:
                    triangle_count += 1
    return triangle_count // 3

def main(input_file_path):
    text_rdd = read_input_file(input_file_path)
    edge_list = extract_edges(text_rdd)
    adj_list = create_adj_list(edge_list)
    nodes_set = find_nodes_set(edge_list)
    nodes_array = np.array(sorted([int(node) for node in nodes_set]))
    num_edges = count_num_edges(edge_list)
    threshold_count = num_edges ** 0.5
    heavy_hitters_count = count_heavy_hitters(adj_list, threshold_count)
    triangles_count = count_triangles(nodes_array, adj_list)
    print("Number of heavy hitter nodes:", heavy_hitters_count)
    print("Number of triangles:", triangles_count)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_file>")
        sys.exit(1)
    input_file_path = sys.argv[1]
    main(input_file_path)
