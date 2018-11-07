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

/**
 * Interface defining a merge rule to be run across a graph
 */
public interface MergeRule {

    /**
     * The label to which this rule applies
     */
    String getLabel();

    /**
     * Return a human readable name for this rule
     */
    default String getRuleName(){
        return "MergeRule[label="+getLabel()+"]";
    }
}
