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

package uk.gov.nca.graph.merge;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.nca.graph.merge.rules.MergeRule;
import uk.gov.nca.graph.merge.rules.PropertiesMergeRule;
import uk.gov.nca.graph.utils.ElementUtils;
import uk.gov.nca.graph.utils.GraphUtils;

/**
 * Utility for merging vertices within a graph.
 *
 * A collection of MergeRules can be provided to control how the merging is done, or alternatively
 * any classes implementing {@link MergeRule} on the classpath will be used if explicit rules are
 * not provided.
 *
 * Currently, only {@link PropertiesMergeRule} are supported.
 */
public class GraphMerger {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphMerger.class);

    private GraphMerger(){}

    /**
     * Merge vertices in a graph using the explicitly provided {@link MergeRule}s
     */
    public static void mergeGraphs(Graph graph, Collection<MergeRule> mergeRules){

        //For now, only handle PropertiesMergeRule
        LOGGER.info("Filtering out rules that don't implement PropertiesMergeRule");
        Collection<PropertiesMergeRule> propertyRules = mergeRules.stream()
            .filter(r -> r instanceof PropertiesMergeRule)
            .map(r -> (PropertiesMergeRule)r)
            .collect(Collectors.toList());
        LOGGER.info("{} PropertiesMergeRules found to apply to graph", propertyRules.size());

        Map<Object, Vertex> mergeMap = new HashMap<>();

        for(PropertiesMergeRule rule : propertyRules) {
            LOGGER.info("Applying rule {} to graph", rule.getRuleName());

            LOGGER.info("Traversing graph to find vertices with label {} and properties {}", rule.getLabel(), rule.getProperties());

            GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().hasLabel(rule.getLabel());
            for(String property : rule.getProperties()){
                traversal = traversal.has(property);
            }

            List<Vertex> vertices = traversal.toList();
            if(vertices.isEmpty()) {
                LOGGER.info("No vertices with label {}, skipping rule {}", rule.getLabel(), rule.getRuleName());
                continue;
            }

            LOGGER.info("Generating merge sets for rule {}", rule.getRuleName());
            ConcurrentMap<List<Object>, List<Vertex>> mergeSets = vertices.parallelStream()
                .collect(Collectors.groupingByConcurrent(rule::getProperties));

            //Filter out mergeSets with a List of nulls
            LOGGER.info("Filtering merge sets for rule {}", rule.getRuleName());
            ConcurrentMap<List<Object>, List<Vertex>> filteredMergeSets = mergeSets.entrySet().parallelStream()
                .filter(e -> e.getKey().stream().anyMatch(Objects::nonNull))
                .filter(e -> e.getValue().size() >= 2)
                .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));

            LOGGER.info("Number of valid merge sets found: {}", filteredMergeSets.size());

            LOGGER.info("Performing merges for vertices for rule {}", rule.getRuleName());

            filteredMergeSets.values().forEach(l -> mergeVertices(graph, l, rule.getLabel(), mergeMap));
        }

        LOGGER.info("Committing changes to graph");
        GraphUtils.commitGraph(graph);

    }

    /**
     * Merge vertices in a graph using the {@link MergeRule}s found on the classpath
     */
    public static void mergeGraphs(Graph graph){
        LOGGER.info("Instantiating merge rules");
        ScanResult sr = new ClassGraph().enableClassInfo().scan();

        List<Class<MergeRule>> mergeRulesClasses = sr.getClassesImplementing(MergeRule.class.getName())
            .loadClasses(MergeRule.class, true);

        List<MergeRule> mergeRules = new ArrayList<>();
        for(Class<MergeRule> clazz : mergeRulesClasses) {
            if(clazz.isAnonymousClass() || Modifier.isAbstract(clazz.getModifiers()))
                continue;

            LOGGER.info("Instantiating rule {}", clazz.getName());
            try {
                mergeRules.add(clazz.getConstructor().newInstance());
            } catch (Exception e) {
                LOGGER.error("Couldn't instantiate rule {}", clazz.getName(), e);
            }
        }

        mergeGraphs(graph, mergeRules);
    }

    private static Vertex mergeVertices(Graph graph, Collection<Vertex> mergeGroup, String label, Map<Object, Vertex> alreadyMerged) {
        LOGGER.debug("Merging group of {} vertices", mergeGroup.size());
        if(mergeGroup.isEmpty())
            return null;

        List<Object> ids = new ArrayList<>();

        Vertex v = graph.addVertex(label);
        for (Vertex orig : mergeGroup) {
            ids.add(orig.id());
            //Copy properties onto v
            LOGGER.debug("Copying properties from vertex {} onto new vertex", orig.id());
            ElementUtils.copyProperties(orig, v);

            List<Edge> newEdges = new ArrayList<>();
            //Copy edges onto v
            for (Edge eOrig : graph.traversal().V(orig.id()).inE().toSet()) {
               if (newEdges.contains(eOrig))
                    continue;

                LOGGER.debug("Copying IN edge {}, from {}", eOrig.id(), eOrig.outVertex().id());
                Vertex vOut = alreadyMerged.getOrDefault(eOrig.outVertex().id(), eOrig.outVertex());

                Edge e = vOut.addEdge(eOrig.label(), v);
                ElementUtils.copyProperties(eOrig, e);

                LOGGER.debug("Recording new edge {}", eOrig.id());
                newEdges.add(e);
            }

            for (Edge eOrig : graph.traversal().V(orig.id()).outE().toSet()) {
                if (newEdges.contains(eOrig))
                    continue;

                LOGGER.debug("Copying OUT edge {}, to {}", eOrig.id(), eOrig.inVertex().id());
                Vertex vIn = alreadyMerged.getOrDefault(eOrig.inVertex().id(), eOrig.inVertex());
                Edge e = v.addEdge(eOrig.label(), vIn);
                ElementUtils.copyProperties(eOrig, e);

                LOGGER.debug("Recording new edge {}", eOrig.id());
                newEdges.add(e);
            }

            LOGGER.debug("{} edges copied", newEdges.size());

            alreadyMerged.put(orig.id(), v);
        }

        if (!ids.isEmpty()) {
            LOGGER.debug("Removing original vertices");
            graph.traversal().V(ids.toArray()).drop().iterate();
        }

        GraphUtils.commitGraph(graph);

        return v;
    }
}
