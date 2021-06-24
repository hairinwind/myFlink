package my.flink.myflinkclient;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.queryablestate.exceptions.UnknownKeyOrNamespaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@RestController
public class MyFlinkClientApplication {

	private static Logger logger = LoggerFactory.getLogger(MyFlinkClientApplication.class);
	private static final String proxyHost = "localhost";
	private static final int proxyPort = 9069;
	private QueryableStateClient client;

	public static void main(String[] args) {
		SpringApplication.run(MyFlinkClientApplication.class, args);
	}

	@GetMapping("/info")
	public String info() {
		return "project to test flink queryable state";
	}

	@GetMapping("/state/{jobId}")
	/* curl localhost:9090/state/239479c9380a0871f9914802e0f7feb8?key=a */
	public Integer getState(@PathVariable String jobId, @RequestParam String key) throws IOException, ExecutionException, InterruptedException {
		return getQueryableState(key, jobId);
	}

	private QueryableStateClient getQueryableStateClient() throws UnknownHostException {
		if (client == null) {
			client = new QueryableStateClient(proxyHost, proxyPort);
		}
		return client;
	}

	public Integer getQueryableState(String key, String jobIdParam) throws IOException, InterruptedException, ExecutionException {
		JobID jobId = JobID.fromHexString(jobIdParam);

		/*
			the ValueStateDescriptor generic type shall be consistent with the queryable state
			In TextStreamWordCountQueryableJob, the state is from counts.keyBy(0).asQueryableState("wordCountState");
			and counts type is Tuple2<String, Integer>
		*/
		ValueStateDescriptor<Tuple2<String, Integer>> stateDescriptor =
				new ValueStateDescriptor<>(
						"",
						TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

		CompletableFuture<ValueState<Tuple2<String, Integer>>> completableFuture =
				getQueryableStateClient().getKvState(
						jobId,
						"wordCountState",
						key,
						BasicTypeInfo.STRING_TYPE_INFO,
						stateDescriptor);

		while(!completableFuture.isDone()) {
			Thread.sleep(100);
		}

		return completableFuture.get().value().f1;

	}

	@GetMapping("/balance/{jobId}")
	public Double getBalanceState(@PathVariable String jobId, @RequestParam String account) throws IOException, ExecutionException, InterruptedException {
		String stateName = "balance";
		return getQueryableBalanceState(account, jobId, stateName);
	}

	@GetMapping("/balance/{jobId}/{stateName}")
	public Double getBalanceState(@PathVariable String jobId, @PathVariable String stateName, @RequestParam String account) throws IOException, ExecutionException, InterruptedException {
		return getQueryableBalanceState(account, jobId, stateName);
	}

	public Double getQueryableBalanceState(String key, String jobIdParam, String stateName) throws IOException, InterruptedException, ExecutionException {
		JobID jobId = JobID.fromHexString(jobIdParam);

		ValueStateDescriptor<Double> stateDescriptor =
				new ValueStateDescriptor<>(
						"",
						TypeInformation.of(new TypeHint<Double>() {}));

		CompletableFuture<ValueState<Double>> completableFuture =
				getQueryableStateClient().getKvState(
						jobId,
						stateName,
						key,
						BasicTypeInfo.STRING_TYPE_INFO,
						stateDescriptor);

		while(!completableFuture.isDone()) {
			Thread.sleep(100);
		}

		try {
			return completableFuture.get().value();
		} catch (ExecutionException ee) {
			if (ee.getCause() instanceof UnknownKeyOrNamespaceException) {
				logger.warn("error to get state of {}, {}", key, ee.getCause().getMessage());
				return 0D;
			}
			throw ee;
		}

	}

}
