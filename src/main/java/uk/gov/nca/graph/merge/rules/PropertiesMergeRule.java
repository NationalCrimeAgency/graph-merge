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

package uk.gov.nca.graph.merge.rules;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import uk.gov.nca.graph.utils.ElementUtils;

/**
 * Interface for defining a MergeRule which merges nodes with matching
 * properties
 */
public interface PropertiesMergeRule extends MergeRule {

  /**
   * List of properties on which to merge
   */
  List<String> getProperties();

  /**
   * Get the property values from a vertex for the properties
   * defined in this rule (order should be consistent with the
   * order returned by getProperties() )
   */
    default List<Object> getProperties(Vertex v){
        return getProperties().stream()
            .map(p -> ElementUtils.getProperty(v, p))
            .collect(Collectors.toList());
    }

  @Override
    default String getRuleName(){
        return "PropertiesMergeRule[label="+getLabel()+",properties="+getProperties()+"]";
    }
}
