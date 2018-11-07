# Graph Merge

Tools to merge vertices within a graph (also known as deduplicating).
Merging is performed based on rules defined by classes inheriting from the MergeRule
interface (these rules are not provided within this library).

To use the command line utility, you should pass a TinkerPop properties file specifying
the connection to the graph. Merging will be performed in place, so you should make a
backup if you wish to retain the unmerged vertices.

    java -cp merge-1.1-shaded.jar uk.gov.nca.graph.merge.cli.MergeGraph -g graphml.properties