package org.apache.giraph.examples;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


import java.io.IOException;

// Giraph 1.1 version

/**
 * Implementation of the HCC algorithm that identifies connected components and
 * assigns each vertex its "component identifier" (the smallest vertex id
 * in the component)
 *
 * The idea behind the algorithm is very simple: propagate the smallest
 * vertex id along the edges to all vertices of a connected component. The
 * number of supersteps necessary is equal to the length of the maximum
 * diameter of all components + 1
 *
 * The original Hadoop-based variant of this algorithm was proposed by Kang,
 * Charalampos, Tsourakakis and Faloutsos in
 * "PEGASUS: Mining Peta-Scale Graphs", 2010
 *
 * http://www.cs.cmu.edu/~ukang/papers/PegasusKAIS.pdf
 */
@Algorithm(
		name = "Connected Visitors",
		description = "Finds connected visitors to identify unique users"
		)
public class ConnectedVisitorsComputation extends
BasicComputation<Text, Text, NullWritable, Text> {
	/**
	 * Propagates the oldest visitor ID to all neighbors based on the value of visit ID. Will always choose to
	 * halt and only reactivate if an older vistor ID has been sent to it.
	 *
	 * @param vertex Vertex
	 * @param messages Iterator of messages from the previous superstep.
	 * @throws IOException
	 */
	@Override
	public void compute(
			Vertex<Text, Text, NullWritable> vertex,
			Iterable<Text> messages) throws IOException {

		Text currentComponent = vertex.getValue();
		// Parse id of current vertex to get visit id
		double currentVisitId = getVisitId(currentComponent);

		// First superstep is special, because we can simply look at the neighbors
		if (getSuperstep() == 0) {
			for (Edge<Text, NullWritable> edge : vertex.getEdges()) {

				Text neighbor = edge.getTargetVertexId();

				// Parse id of connected vertex to get visit id
				double neighborVisitId = getVisitId(neighbor);

				if (neighborVisitId < currentVisitId) {
					currentComponent = neighbor;
				}
			}

			// Only need to send value if it is not the own id
			if (currentComponent != vertex.getValue()) {
				vertex.setValue(new Text(currentComponent));
				for (Edge<Text, NullWritable> edge : vertex.getEdges()) {

					Text neighbor = edge.getTargetVertexId();

					// Parse id of connected vertex to get visit id
					double neighborVisitId = getVisitId(neighbor);

					if (neighborVisitId > currentVisitId) {
						sendMessage(neighbor, vertex.getValue());
					}
				}
			}

			vertex.voteToHalt();
			return;
		}

		boolean changed = false;
		
		// did we get a smaller id ?
		for (Text message : messages) {
			
			Text candidateComponent = message;

			// Parse id of message to get visit id
			double candidateVisitId = getVisitId(candidateComponent);

			if (candidateVisitId < currentVisitId) {
				currentComponent = candidateComponent;
				changed = true;
			}
		}

		// propagate new component id to the neighbors
		if (changed) {
			vertex.setValue(new Text(currentComponent));
			sendMessageToAllEdges(vertex, vertex.getValue());
		}
		vertex.voteToHalt();
	}

	private double getVisitId(Text value){
		// Split string into separate values
		String[] valueArray = value.toString().split("_"); 
		
		double visitId;		
		if(valueArray.length == 3){
			// Get visit id (unix timestamp)
			visitId = Double.parseDouble(valueArray[2]);
		} else {
			// If no visit id then set value to very high number 
			visitId = 9.99e18;
		}		
		return visitId;
	}
}
