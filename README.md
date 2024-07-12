# Shortest-Path-Calculator
This project demonstrates how to process a graph using Hadoop MapReduce to find the shortest paths from a starting node to all other nodes in the graph. The graph is represented in an adjacency list format, and the algorithm used for finding the shortest paths is a variant of the Breadth-First Search (BFS).

## Prerequisites

Before running this project, ensure you have the following installed:

- Java Development Kit (JDK)
- Apache Hadoop
- Apache Maven (optional, for building the project)

## Project Structure

- **Graph.java**: The main Java file containing Mapper and Reducer classes and the `main` method to set up and run the MapReduce jobs.
- **Tagged.java**: A custom Writable class to store the distance and following nodes of a graph node.

## Running the Project

### Step 1: Compile the Java Code

First, compile the Java code using `javac`:

```sh
javac -classpath $(hadoop classpath) -d . Tagged.java Graph.java
```

### Step 2: Create a JAR File

Next, create a JAR file containing the compiled classes:

```sh
jar -cvf graph-processing.jar -C . .
```

### Step 3: Prepare Input Data

The input data should be a text file where each line represents a directed edge in the graph in the format: `node_id,follower_id`.

For example:

```
1,2
2,3
3,4
4,1
```

### Step 4: Run the MapReduce Jobs

Run the MapReduce jobs using Hadoop:

```sh
hadoop jar graph-processing.jar Graph <input_path> <output_path>
```

Here, `<input_path>` is the HDFS path to the input data file, and `<output_path>` is the HDFS path where the output will be stored.

### Detailed Explanation

#### First MapReduce Job (Graph Parsing)

The first MapReduce job reads the input graph and outputs key-value pairs where the key is the follower ID and the value is the node ID.

**Mapper (FirstMapper)**:
- Input: Text (CSV line)
- Output: (Follower ID, Node ID)

**Reducer (FirstReducer)**:
- Input: (Follower ID, List of Node IDs)
- Output: (Follower ID, Tagged Object)

#### Second MapReduce Job (Shortest Path Calculation)

The second MapReduce job iterates over the graph, propagating the shortest path distance from the start node to all other nodes.

**Mapper (SecondMapper)**:
- Input: (Node ID, Tagged Object)
- Output: (Node ID, Tagged Object)

**Reducer (SecondReducer)**:
- Input: (Node ID, List of Tagged Objects)
- Output: (Node ID, Tagged Object)

This job is run multiple times (defined by the `iterations` variable) to ensure the shortest paths are fully propagated throughout the graph.

#### Third MapReduce Job (Final Output)

The third MapReduce job outputs the final shortest path distances from the start node to each node.

**Mapper (ThirdMapper)**:
- Input: (Node ID, Tagged Object)
- Output: (Node ID, Distance)

**Reducer (ThirdReducer)**:
- Input: (Node ID, List of Distances)
- Output: (Node ID, Distance)
