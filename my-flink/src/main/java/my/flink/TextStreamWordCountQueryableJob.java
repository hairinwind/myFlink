package my.flink;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * This example shows an implementation of WordCount with data from a text
 * socket. To run the example make sure that the service providing the text data
 * is already up and running.
 * 
 * <p>
 * To start an example socket text stream on your local machine run netcat from
 * a command line: <code>nc -lk 9999</code>, where the parameter specifies the
 * port number.
 * 
 * 
 * <p>
 * Usage:
 * <code>SocketTextStreamWordCount &lt;hostname&gt; &lt;port&gt;</code>
 * <br>
 * 
 * <p>
 * This example shows how to:
 * <ul>
 * <li>use StreamExecutionEnvironment.socketTextStream
 * <li>write a simple Flink program
 * <li>write and use user-defined functions
 * </ul>
 *
 *
 * To test it,
 * run this class, then type "a" on nc console a couple of times
 * the jobId is output on the console
 * run MyFlinkClientApplication
 * curl localhost:9090/state/239479c9380a0871f9914802e0f7feb8?key=a
 */
public class TextStreamWordCountQueryableJob {

	public static void main(String[] args) throws Exception {
		String hostName;
		Integer port;
		if (args.length != 2){
			hostName = "localhost";
			port = 9000;
		} else {
			hostName = args[0];
			port = Integer.parseInt(args[1]);
		}

		// set up the execution environment
		Configuration config = new Configuration();
		config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

		// get input data
		DataStream<String> text = env.socketTextStream(hostName, port);

		DataStream<Tuple2<String, Integer>> counts =
			// split up the lines in pairs (2-tuples) containing: (word,1)
			text.flatMap(new LineSplitter())
			// group by the tuple field "0" and sum up tuple field "1"
					.keyBy(0)
					.sum(1);

		text.map(new MyMapper());

		counts.print();

		//queryable state
		counts.keyBy(0).asQueryableState("wordCountState");

		// execute program
		env.execute("Java WordCount from SocketTextStream Example");
	}

	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}

		}
	}

	public static final class MyMapper extends RichMapFunction<String, String> {

		@Override
		public String map(String s) throws Exception {
			System.out.println("jobId: " + getRuntimeContext().getJobId());
			return s;
		}
	}
}
