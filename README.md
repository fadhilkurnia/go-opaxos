# OPaxos - Oblivious Paxos

Oblivious Paxos (OPaxos) is a privacy-preserving consensus protocol 
integrating Secret Sharing into Paxos family of consensus protocol. 
The usage of Secret Sharing provides information-theoretic privacy,
which protects against adversaries with unbounded computational power,
as long as they do not collude with one another.

Additionally, we develop Fast Oblivious Paxos (Fast-OPaxos) 
protocol that enables non-leader node (or client) to directly
propose value to the acceptors (backups), without leader 
involvement. Fast-OPaxos is suitable for deployment under low conflict
rate.

Our prototype use an efficient secret-sharing library accessible in https://github.com/fadhilkurnia/shamir.
The library extends the secret-sharing component of Hashicorp Vault.

Please cite our paper titled 
"Oblivious Paxos: Privacy-Preserving Consensus Over Secret Shares" 
below 😊
```
@inproceedings{kurnia:socc23:opaxos,
   author = {Kurnia, Fadhil I. and Venkataramani, Arun},
   title = {Oblivious Paxos: Privacy-Preserving Consensus Over Secret Shares},
   year = {2023},
   isbn = {9798400703874},
   publisher = {Association for Computing Machinery},
   address = {New York, NY, USA},
   url = {https://doi.org/10.1145/3620678.3624647},
   doi = {10.1145/3620678.3624647},
   booktitle = {Proceedings of the 2023 ACM Symposium on Cloud Computing},
   pages = {65–80},
   numpages = {16},
   keywords = {Distributed Consensus, Secret Sharing, Privacy},
   location = {Santa Cruz, CA, USA},
   series = {SoCC '23}
}
```
DOI: https://doi.org/10.1145/3620678.3624647

## Quick Run
### Installation and Compilation
1. Download Go 1.19, then extract and install it.
```bash
wget https://go.dev/dl/go1.19.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.19.linux-amd64.tar.gz
echo "export PATH=$PATH:/usr/local/go/bin" | sudo tee -a /etc/environment
source /etc/environment

# test the installation
go version
```
If you use different OS (e.g., MacOS) or different architecture (e.g., ARM), please
change the downloaded Go file accordingly.

2. Clone the OPaxos repository, then compile it.
```bash
git clone https://github.com/opaxos/opaxos.git
cd opaxos/bin
./build.sh
```

After compilation, we will have 3 executable files under `bin` folder.
* `server` is one consensus node instance.
* `client` is benchmark client that generates read/write request to servers.
* `cmd` is a command line tool to test Get/Set requests.

### Run OPaxos Server Nodes

1. **Observe the configuration file** in `bin/config-opaxos.json`. 
   It should contain something like the snippet below. 
   The default configuration, runs 5 server nodes, which consist of 2 trusted nodes and 3 untrusted nodes.
```json
{
  ...
  "address": {
    "1.1": "tcp://127.0.0.1:1735",
    "1.2": "tcp://127.0.0.1:1736",
    "1.3": "tcp://127.0.0.1:1737",
    "1.4": "tcp://127.0.0.1:1738",
    "1.5": "tcp://127.0.0.1:1739"
  },
  ...
  "roles": {
    "1.1": "proposer,acceptor,learner",
    "1.2": "proposer,acceptor,learner",
    "1.3": "acceptor,learner",
    "1.4": "acceptor,learner",
    "1.5": "acceptor,learner"
  },
  "protocol": {
    "name": "opaxos",
    "secret_sharing": "shamir",
    "threshold": 2,
    "quorum_1": 4,
    "quorum_2": 3
  },
}
```

2. **Start the 5 server nodes with different ID**. You can run them each in a separate terminal tab or 
   alternatively run them in the background by adding `&` at the end of the command.
```
./server -id 1.1 -algorithm opaxos -config config-opaxos.json -log_stdout
./server -id 1.2 -algorithm opaxos -config config-opaxos.json -log_stdout
./server -id 1.3 -algorithm opaxos -config config-opaxos.json -log_stdout
./server -id 1.4 -algorithm opaxos -config config-opaxos.json -log_stdout
./server -id 1.5 -algorithm opaxos -config config-opaxos.json -log_stdout
```

**What did we just run?** 

With the provided default configuration, we are running the
architecture illustrated below. Note that by default, the nodes communicate with Unix socket, you can
change this to TCP by using the `-client_type tcp` flag argument.
The `-log_stdout` flag make the server to output the log into the standard out (terminal), 
without that flag, by default, the server write the log into `server.<pid>.log` file.

```
          acceptor
acceptor  ┌─────┐  acceptor
 ┌─────┐  │     │  ┌─────┐
 │     │  │     │  │     │
 │     │  │ 1.4 │  │     │
 │ 1.3 │  │     │  │ 1.5 │    => 3 untrusted nodes
 │     │  └─────┘  │     │         (honest but curious)
 └─────┘           └─────┘


     ┌─────┐  ┌─────┐
     │     │  │     │
     │     │  │     │         => 2 trusted nodes
     │ 1.1 │  │ 1.2 │
     │     │  │     │
     └─────┘  └─────┘
     proposer  proposer
     acceptor  acceptor
     (leader)
```

### Run OPaxos Client

To run a CLI client, simply run `cmd` with the target server node.
```
./cmd -id 1.1 -config config-opaxos.json
```
In the command above, the target server node is `1.1`, the node is going to be the leader.
Then, in the CLI, simply enter Put/Get commands. For example:
```
paxi $ put 313 S3CR3T
paxi $ get 313
>> S3CR3T
```
The underlying system is a linearizable and privacy-preserving Key-Value Store with integer Key and string Value.

### Run benchmark client

There is also benchmark client that send a bulk of commands to the Key-Value Store.
Use the following command to run the benchmark client.
```
./client -id 1.1 -config config-opaxos.json -log_stdout
```
The command runs the default benchmark client and will output the measured statistics as follows:
```
[INFO] 2023/10/31 16:47:25.711480 benchmark.go:682: Client-1 runtime = 57.775792253s
[INFO] 2023/10/31 16:47:25.711513 benchmark.go:683: Client-1 request-rate = 1730.828710
[INFO] 2023/10/31 16:47:25.746924 benchmark.go:695: Concurrency = 1
[INFO] 2023/10/31 16:47:25.746942 benchmark.go:696: Write Ratio = 1.000000
[INFO] 2023/10/31 16:47:25.746949 benchmark.go:697: Number of Keys = 9
[INFO] 2023/10/31 16:47:25.746957 benchmark.go:698: Benchmark Time = 57.776099936s
[INFO] 2023/10/31 16:47:25.746963 benchmark.go:699: Throughput = 1730.819493
[INFO] 2023/10/31 16:47:25.746984 benchmark.go:700: size = 100000
mean = 0.575717
stddev = 0.094068
min = 0.435383
max = 2.198561
median = 0.553390
p95 = 0.728911
p99 = 1.006410
p999 = 1.297779
```

**What did we just run?**

We run the default benchmark client as specified in the `config-opaxos.json` configuration file.
The default benchmark client has 1 client instance and sends 100 000 blocking Put requests, each with a value of 10 bytes.
The default benchmark client uses `-client_action=block` which means the client
will wait for a response from a previous request before issuing the next request; 
i.e., there is only one outstanding request at a time (or low load).

For measurement under higher load (i.e., more than one outstanding requests at any given time), 
you can use the `-client_action=pipeline` flag. 

Here are the complete action type of benchmark client:
- `block`: wait for a response from the previous request before issuing a new request.
- `pipeline`: directly pipeline the requests; i.e., send a request without having to wait responses from previous requests. The latency is measured by putting the start time in the request payload. 
- `callback`: similar to pipeline, but the client use callback function for each request to capture the 
- `throughput_collector`: run client while recording the throughput every 1 second and output it in the log that by default is written to an external file (in the `client.<pid>.log` when `-log_stdout=false`).
- `tracefile`: run client that issue request based on the provided tracefile. This is useful when we want to run standard benchmark such as YCSB.

Some important benchmark configuration:
- "T" indicates the duration of the benchmark.
- "N" indicates the number of issued requests.
- "K" indicates the highest integer Key, started from 1.
- "W" indicates the ratio of write requests, max is 1.0.
- "Size" indicates the size of value in bytes for write requests.
- "Throttle" indicates the target load the client instances need to send.
- "Concurrency" indicates the number of client instances.

> Note: For Fast-OPaxos, please use `-algorithm=fastopaxos` 
> and `-client_type=tcp` for both the `server` and `client` program
> with no concurrent client.

## Distributed Run
We also provide the scripts to run OPaxos in actual distributed machines, 
including scripts to generate some graphs shown in our paper.
All the important variables to run all the distributed scripts are located in the `variables.sh` file.
Please check the instructions to reproduce the graphs in the Wiki page.

## Model Checker
Additionally, we provide the TLA+ specification of OPaxos and model check 
the protocol to ensure its safety property. Please check the TLA+ code in the Wiki page. 

## Acknowledgement
This prototype of OPaxos and Fast-OPaxos is possible due to 
the original implementation of Paxi by 
Ailidani Ailijiang, Aleksey Charapko, and Murat Demirbas, 
that make consensus protocol development easier. 
Please check and cite Paxi on https://github.com/ailidani/paxi.


> Warning: this implementation is a research prototype and not 
> suitable for production usage that require certified security guarantee. 
> So, use it wisely 😉