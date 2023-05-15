#!/usr/bin/python3
import argparse
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.patches as mpatches

parser = argparse.ArgumentParser(description='experiments script')
parser.add_argument('-l', "--location", type=str, required=True)
args = parser.parse_args()

fig=plt.figure()
plt_load1 = plt.subplot(121)
plt_load2 = plt.subplot(122)
fig.set_size_inches(6.4, 1.8)
fig.subplots_adjust(wspace=0.2)
fig.subplots_adjust(left=0.0,right=1.0,bottom=0,top=0.75)

colors = ['#1f77b4', '#ff7f0e', '#9F2B68']
colors2 = ['#801f77b4', '#80ff7f0e', '#809F2B68']
labels = ['Paxos', 'OPaxos', 'Fast-OPaxos']
x = np.arange(len(labels))
width = 0.3

workdir = args.location

def get_data(dataset, client_location, protocol, ss_algorithm):
    data = np.loadtxt("{}/latency_{}_{}_{}_{}.dat".format(workdir, dataset, client_location, protocol, ss_algorithm))
    return data

# dataset-1: pingpong
remote_data_paxos = get_data("pingpong", "remote", "paxos", "")
remote_data_opaxos = get_data("pingpong", "remote", "opaxos", "shamir")
remote_data_fastopaxos = get_data("pingpong", "remote", "fastopaxos", "shamir")
remote_latencies = [np.mean(remote_data_paxos), np.mean(remote_data_opaxos), np.mean(remote_data_fastopaxos)]
remote_stdev = [np.std(remote_data_paxos), np.std(remote_data_opaxos), np.std(remote_data_fastopaxos)]
inhouse_data_paxos = get_data("pingpong", "inhouse", "paxos", "")
inhouse_data_opaxos = get_data("pingpong", "inhouse", "opaxos", "shamir")
inhouse_data_fastopaxos = get_data("pingpong", "inhouse", "fastopaxos", "shamir")
inhouse_latencies = [np.mean(inhouse_data_paxos), np.mean(inhouse_data_opaxos), np.mean(inhouse_data_fastopaxos)]
inhouse_stdev = [np.std(inhouse_data_paxos), np.std(inhouse_data_opaxos), np.std(inhouse_data_fastopaxos)]
plt_load1.bar(x-width/2, inhouse_latencies, width, yerr=[np.zeros(len(labels)), inhouse_stdev], capsize=3, edgecolor='black', zorder=3, label='In-house Client', color=colors, alpha=1, hatch='xx')
plt_load1.bar(x+width/2, remote_latencies, width, yerr=[np.zeros(len(labels)), remote_stdev], capsize=3, edgecolor='black', zorder=3, label='Remote Client', color=colors, hatch='...', alpha=0.5)
plt_load1.set_xticks(x)
plt_load1.set_xticklabels(labels)
plt_load1.set_xlabel("PingPong dataset")
plt_load1.set_ylabel("Avg Latency (ms)")
plt_load1.grid(zorder=0)

# dataset-2: moniotr
remote_data_paxos = get_data("moniotr", "remote", "paxos", "")
remote_data_opaxos = get_data("moniotr", "remote", "opaxos", "shamir")
remote_data_fastopaxos = get_data("moniotr", "remote", "fastopaxos", "shamir")
remote_latencies = [np.mean(remote_data_paxos), np.mean(remote_data_opaxos), np.mean(remote_data_fastopaxos)]
remote_stdev = [np.std(remote_data_paxos), np.std(remote_data_opaxos), np.std(remote_data_fastopaxos)]
inhouse_data_paxos = get_data("moniotr", "inhouse", "paxos", "")
inhouse_data_opaxos = get_data("moniotr", "inhouse", "opaxos", "shamir")
inhouse_data_fastopaxos = get_data("moniotr", "inhouse", "fastopaxos", "shamir")
inhouse_latencies = [np.mean(inhouse_data_paxos), np.mean(inhouse_data_opaxos), np.mean(inhouse_data_fastopaxos)]
inhouse_stdev = [np.std(inhouse_data_paxos), np.std(inhouse_data_opaxos), np.std(inhouse_data_fastopaxos)]
plt_load2.bar(x-width/2, inhouse_latencies, width, yerr=[np.zeros(len(labels)), inhouse_stdev], capsize=3, edgecolor='black', zorder=3, color=colors, alpha=1, hatch='xx')
plt_load2.bar(x+width/2, remote_latencies, width, yerr=[np.zeros(len(labels)), remote_stdev], capsize=3, edgecolor='black', zorder=3, color=colors, hatch='...', alpha=0.5)
plt_load2.set_xticks(x)
plt_load2.set_xticklabels(labels)
plt_load2.set_xlabel("Mon(IoT)r dataset")
plt_load2.grid(zorder=0)

# legend
# handles, labels = plt_load1.get_legend_handles_labels()
# fig.legend(handles, labels, loc='upper center', ncol=3, facecolor='white')
ihc_legend = mpatches.Patch(facecolor='white', hatch='xxx', edgecolor='black', label='In-house Client')
rmt_legend = mpatches.Patch(facecolor='white', hatch='....', edgecolor='black', label='Remote Client')
fig.legend(handles=[ihc_legend, rmt_legend],loc='upper center', ncol=3)

plt.savefig(workdir + "/latency_casestudy", bbox_inches='tight')
    
fig_loc = workdir + "/latency_casestudy.png"
print(">> ✅ 🎉 please check the result in {}".format(fig_loc))