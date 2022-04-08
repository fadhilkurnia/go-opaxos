## Configuration Files

This folder contains configuration file and configuration template used for evaluation purposes.


### Configurations for Latency Measurement

Assuming the 80% load is 16000 req/s. For the lowload measurement, we use a single blocking unix client. For the 80% load measurement, we use pipelined unix clients.

```
cloudlab_lat_lowload_paxos.json: one outstanding request, 10k requests.
cloudlab_lat_lowload_opaxos.json: one outstanding request, 10k requests.
cloudlab_lat_lowload_opaxos_ssms.json: one outstanding request, 10k requests.
cloudlab_lat_80load_paxos.json: 10 parallel clients, each with 1600 reqs/sec.
cloudlab_lat_80load_opaxos.json: 10 parallel clients, each with 1600 reqs/sec.
cloudlab_lat_80load_opaxos_ssms.json: 10 parallel clients, each with 1600 reqs/sec.
```

### Configuration Templates for Capacity and Sensitivity Evaluation
```
cloudlab_cap_paxos_template.json
cloudlab_cap_opaxos_template.json
cloudlab_cap_opaxos_ssms_template.json
```