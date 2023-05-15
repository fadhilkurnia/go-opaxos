#!/usr/bin/python3
import argparse
import matplotlib.pyplot as plt
import numpy as np

parser = argparse.ArgumentParser(description='experiments script')
parser.add_argument('-l', "--location", type=str, required=True)
args = parser.parse_args()

# gather the data
quorums = ['(2,5)', '(3,4)', '(4,3)', '(5,2)']
algorithm = ['paxos_', 'opaxos_shamir', 'opaxos_ssms']
output_loc = args.location


paxos_lats = []
paxos_stddev = []
opaxos_shamir_lats = []
opaxos_shamir_stddev = []
opaxos_ssms_lats = []
opaxos_ssms_stddev = []
for alg in algorithm:
    for i, q in enumerate(quorums):
        sq = q[1:-1]
        q1 = sq.split(",")[0]
        q2 = sq.split(",")[1]
        lat = np.loadtxt("{}/quorum_lat_{}_{}_{}.dat".format(output_loc, alg, q1, q2))

        if alg == "paxos_":
            paxos_lats.append(np.average(lat))
            paxos_stddev.append(np.std(lat))
        elif alg == "opaxos_shamir":
            opaxos_shamir_lats.append(np.average(lat))
            opaxos_shamir_stddev.append(np.std(lat))
        elif alg == "opaxos_ssms":
            opaxos_ssms_lats.append(np.average(lat))
            opaxos_ssms_stddev.append(np.std(lat))
            
        continue

norm_paxos_stddev = np.array(paxos_stddev)/np.array(paxos_lats)
norm_opaxos_shamir_stddev = np.array(opaxos_shamir_stddev)/np.array(paxos_lats)
norm_opaxos_ssms_stddev = np.array(opaxos_ssms_stddev)/np.array(paxos_lats)
norm_paxos_loads = np.array(paxos_lats)/np.array(paxos_lats)
norm_opaxos_shamir_loads = np.array(opaxos_shamir_lats)/np.array(paxos_lats)
norm_opaxos_krawczyk_loads = np.array(opaxos_ssms_lats)/np.array(paxos_lats)

fig=plt.figure()
fig.set_size_inches(6, 1.7)
fig.subplots_adjust(hspace=0.5)
fig.subplots_adjust(left=0.0,right=1.0,bottom=0,top=0.75)
plt_quorum  = plt.subplot(111)

ind = [x for x, _ in enumerate(quorums)]
plt_quorum.errorbar(ind, norm_paxos_loads, yerr=norm_paxos_stddev, capsize=3, fmt='*-', label = 'Multi-Paxos')
plt_quorum.errorbar(ind, norm_opaxos_shamir_loads, yerr=norm_opaxos_shamir_stddev, capsize=3, fmt='*-', label = 'Multi-OPaxos (Shamir)')
plt_quorum.errorbar(ind, norm_opaxos_krawczyk_loads, yerr=norm_opaxos_ssms_stddev, capsize=3, fmt='*-', label = 'Multi-OPaxos (SSMS)')

maxtop = np.max([norm_paxos_loads + norm_paxos_stddev + 0.1, norm_opaxos_shamir_loads + norm_opaxos_shamir_stddev + 0.1, norm_opaxos_krawczyk_loads + norm_opaxos_ssms_stddev + 0.1])
plt_quorum.set_ylim(bottom=0.0, top=maxtop)
plt_quorum.set_xticks(ind)
plt_quorum.set_xticklabels(quorums)
plt_quorum.set_xlabel('Quorum size (Q1,Q2)')
plt_quorum.set_ylabel('Norm. Avg. Latency')
plt_quorum.grid()

handles, labels = plt_quorum.get_legend_handles_labels()
fig.legend(handles, labels, loc='upper center', ncol=3)
plt.savefig(output_loc + "/quorum_latency", bbox_inches='tight')
fig_loc = output_loc + "/quorum_latency.png"
print("please check the result in {}".format(fig_loc))