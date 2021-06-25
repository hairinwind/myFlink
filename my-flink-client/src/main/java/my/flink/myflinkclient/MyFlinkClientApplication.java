package my.flink.myflinkclient;

import my.kafka.bank.message.AccountBalance;
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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootApplication
@RestController
public class MyFlinkClientApplication {

	private static Logger logger = LoggerFactory.getLogger(MyFlinkClientApplication.class);
	private static final String proxyHost = "localhost";
	private static final int proxyPort = 9069;
	private static QueryableStateClient client = getQueryableStateClient();

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

	private static QueryableStateClient getQueryableStateClient() {
		try {
			return new QueryableStateClient(proxyHost, proxyPort);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
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
				client.getKvState(
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
	public Optional<AccountBalance> getBalanceState(@PathVariable String jobId, @RequestParam String account) {
		String stateName = "balance";
		return getQueryableBalanceState(Arrays.asList(account), jobId, stateName).stream().findFirst();
	}

	@GetMapping("/balance/{jobId}/{stateName}")
	public Optional<AccountBalance> getBalanceState(@PathVariable String jobId, @PathVariable String stateName, @RequestParam String account)  {
		return getQueryableBalanceState(Arrays.asList(account), jobId, stateName).stream().findFirst();
	}

	@GetMapping("/balanceByAccountRange/{jobId}")
	/**
	 * accountRange is like "100001-100010"
	 */
	public List<AccountBalance> getBalanceStateByAccountRange(@PathVariable String jobId, @RequestParam String accountRange) {
		List<String> accounts = getAccountListByRange(accountRange);
		String stateName = "balance";
		return getQueryableBalanceState(accounts, jobId, stateName);
	}

	private List<String> getAccountListByRange(String accountRange) {
		String[] accounts = accountRange.split("-");
		int startAccount = Integer.valueOf(accounts[0]);
		int endAccount = Integer.valueOf(accounts[1]);
		return IntStream.rangeClosed(startAccount, endAccount).boxed()
				.map(String::valueOf)
				.collect(Collectors.toList());
	}

	private List<AccountBalance> getQueryableBalanceState(List<String> accounts, String jobId, String stateName) {
		return accounts.stream().map(account -> getQueryableBalanceStateInternal(account, jobId, stateName))
				.map(CompletableFuture::join) /* join all CompletableFuture result */
				.collect(Collectors.toList());
	}

	public CompletableFuture<AccountBalance> getQueryableBalanceStateInternal(String key, String jobIdParam, String stateName) {
		JobID jobId = JobID.fromHexString(jobIdParam);

		ValueStateDescriptor<Double> stateDescriptor =
				new ValueStateDescriptor<>(
						"",
						TypeInformation.of(new TypeHint<Double>() {}));

		CompletableFuture<ValueState<Double>> completableFuture =
				client.getKvState(
						jobId,
						stateName,
						key,
						BasicTypeInfo.STRING_TYPE_INFO,
						stateDescriptor);
		return completableFuture.thenApply(balance -> {
			try {
				return new AccountBalance(key, balance.value());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}).exceptionally(ex -> {
			if (ex.getCause() instanceof UnknownKeyOrNamespaceException) {
				return new AccountBalance(key, null);
			}
			throw new RuntimeException(ex);
		});
	}

}
