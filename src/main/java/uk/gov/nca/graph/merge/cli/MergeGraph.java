/*
National Crime Agency (c) Crown Copyright 2018

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package uk.gov.nca.graph.merge.cli;

import static uk.gov.nca.graph.utils.cli.CommandLineUtils.createRequiredOption;
import static uk.gov.nca.graph.utils.cli.CommandLineUtils.parseCommandLine;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.nca.graph.merge.GraphMerger;
import uk.gov.nca.graph.utils.GraphUtils;
import uk.gov.nca.graph.utils.cli.ImportGraph;

public class MergeGraph {
    private static final Logger LOGGER = LoggerFactory.getLogger(ImportGraph.class);

    public static void main(String[] args){
        Options options = new Options();

        options.addOption(createRequiredOption("g", "graph", true, "Configuration file to connect to Gremlin graph"));

        CommandLine cmd = parseCommandLine(args, options, MergeGraph.class, "Merge nodes within a single graph");
        if(cmd == null)
            return;

        LOGGER.info("Connecting to Gremlin graph");
        Graph graph = GraphFactory.open(cmd.getOptionValue('g'));

        GraphMerger.mergeGraphs(graph);

        LOGGER.info("Closing connection to Gremlin graph");
        GraphUtils.closeGraph(graph);

        LOGGER.info("Finished merging graphs");
    }
}
