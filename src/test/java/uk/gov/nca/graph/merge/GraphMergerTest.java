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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.gov.nca.graph.utils.ElementUtils.getProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;
import uk.gov.nca.graph.merge.rules.MergePersonOnSameAs;
import uk.gov.nca.graph.merge.rules.MergeRule;
import uk.gov.nca.graph.merge.rules.PropertiesMergeRule;

public class GraphMergerTest {
    @Test
    public void testMerge() throws Exception{
        Graph graph = TinkerGraph.open();
        Vertex vP1 = graph.addVertex(T.label, "Person", "name", "James");
        Vertex vE1 = graph.addVertex(T.label, "Email", "identifier", "james@example.com");
        vP1.addEdge("email", vE1);

        Vertex vP2 = graph.addVertex(T.label, "Person", "name", "Simon", "sameAs", "http://www.example.com/simon");
        Vertex vE2 = graph.addVertex(T.label, "Email", "identifier", "simon@example.com");
        vP2.addEdge("email", vE2);

        Vertex vP3 = graph.addVertex(T.label, "Person", "name", "Jim");
        Vertex vE3 = graph.addVertex(T.label, "Email", "identifier", "james@example.com");
        vP3.addEdge("email", vE3);

        Vertex vP4 = graph.addVertex(T.label, "Person", "name", "Si", "sameAs", "http://www.example.com/simon");
        Vertex vE4 = graph.addVertex(T.label, "Email", "identifier", "simon@foo.com");
        vP4.addEdge("email", vE4);

        Vertex vI1 = graph.addVertex(T.label, "IPAddress", "identifier", "127.0.0.1");
        Vertex vI2 = graph.addVertex(T.label, "IPAddress", "identifier", "127.0.0.2");
        Vertex vI3 = graph.addVertex(T.label, "IPAddress", "identifier", "127.0.0.1");

        vP1.addEdge("uses", vI1);
        vP2.addEdge("uses", vI2);
        vP3.addEdge("uses", vI3);

        GraphMerger.mergeGraphs(graph);

        //People
        List<Vertex> peopleVertices = new ArrayList<>();
        graph.traversal().V().has(T.label, "Person").fill(peopleVertices);
        assertEquals(3, peopleVertices.size());

        List<String> people = new ArrayList<>();
        peopleVertices.forEach(v -> people.add(getProperty(v,"name").toString()));

        assertTrue(people.contains("James"));
        assertTrue(people.contains("Jim"));
        assertTrue(people.contains("[Si, Simon]"));

        //IPAddresses
        List<Vertex> ipVertices = new ArrayList<>();
        graph.traversal().V().has(T.label, "IPAddress").fill(ipVertices);
        assertEquals(2, ipVertices.size());

        Vertex i1 = ipVertices.get(0);
        assertEquals("127.0.0.1", getProperty(i1,"identifier"));

        Vertex i2 = ipVertices.get(1);
        assertEquals("127.0.0.2", getProperty(i2,"identifier"));

        //Email
        List<Vertex> emailVertices = new ArrayList<>();
        graph.traversal().V().has(T.label, "Email").fill(emailVertices);
        assertEquals(3, emailVertices.size());

        List<String> emails = new ArrayList<>();
        emailVertices.forEach(v -> emails.add(getProperty(v, "identifier").toString()));

        assertTrue(emails.contains("james@example.com"));
        assertTrue(emails.contains("simon@example.com"));
        assertTrue(emails.contains("simon@foo.com"));

        //Edges
        List<Edge> edgesUses = new ArrayList<>();
        graph.traversal().E().has(T.label, "uses").fill(edgesUses);

        assertEquals(3, edgesUses.size());

        List<String> edgesUsesDescs = new ArrayList<>();
        edgesUses.forEach(e -> edgesUsesDescs.add(getProperty(e.outVertex(),"name") +"--"+e.label()+"->"+getProperty(e.inVertex(), "identifier")));

        assertTrue(edgesUsesDescs.contains("James--uses->127.0.0.1"));
        assertTrue(edgesUsesDescs.contains("Jim--uses->127.0.0.1"));
        assertTrue(edgesUsesDescs.contains("[Si, Simon]--uses->127.0.0.2"));

        List<Edge> edgesEmail = new ArrayList<>();
        graph.traversal().E().has(T.label, "email").fill(edgesEmail);

        assertEquals(4, edgesEmail.size());

        List<String> edgesEmailDescs = new ArrayList<>();
        edgesEmail.forEach(e -> edgesEmailDescs.add(getProperty(e.outVertex(),"name") +"--"+e.label()+"->"+getProperty(e.inVertex(), "identifier")));

        assertTrue(edgesEmailDescs.contains("James--email->james@example.com"));
        assertTrue(edgesEmailDescs.contains("Jim--email->james@example.com"));
        assertTrue(edgesEmailDescs.contains("[Si, Simon]--email->simon@example.com"));
        assertTrue(edgesEmailDescs.contains("[Si, Simon]--email->simon@foo.com"));

        graph.close();
    }

    @Test
    public void testSameTypeMerge() throws Exception{
        Graph graph = TinkerGraph.open();
        Vertex vP1 = graph.addVertex(T.label, "Person", "name", "Peter");
        Vertex vP2 = graph.addVertex(T.label, "Person", "name", "Martha", "sameAs", "http://www.example.com/Martha");
        Vertex vP3 = graph.addVertex(T.label, "Person", "name", "Sally");
        Vertex vP4 = graph.addVertex(T.label, "Person", "name", "Martha", "sameAs", "http://www.example.com/Martha");
        Vertex vP5 = graph.addVertex(T.label, "Person", "name", "Martha");

        vP1.addEdge("married", vP2);
        vP1.addEdge("fatherOf", vP3);
        vP4.addEdge("married", vP1);
        vP5.addEdge("motherOf", vP3);

        MergeRule mergeOnPersonName = new PropertiesMergeRule() {
            @Override
            public List<String> getProperties() {
                return Arrays.asList("name");
            }

            @Override
            public String getLabel() {
                return "Person";
            }
        };

        GraphMerger.mergeGraphs(graph, Arrays.asList(new MergePersonOnSameAs(), mergeOnPersonName));

        assertEquals(Long.valueOf(3), graph.traversal().V().count().next());
        assertEquals(Long.valueOf(4), graph.traversal().E().count().next());

        assertTrue(graph.traversal().V().has("name", "Peter").hasNext());
        assertTrue(graph.traversal().V().has("name", "Martha").hasNext());
        assertTrue(graph.traversal().V().has("name", "Sally").hasNext());

        graph.close();
    }

}
