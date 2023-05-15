#!/usr/bin/python3
import argparse
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

parser = argparse.ArgumentParser(description='experiments script')
parser.add_argument('-l', "--location", type=str, required=True)
parser.add_argument('-t', "--top", type=int, default=80)
parser.add_argument('-t2', "--top2", type=int, default=65)
parser.add_argument('-r', "--right", type=int, default=50000)
args = parser.parse_args()

load_diff_at_cap = 1.0

result_df = pd.read_csv("{}/capacity.csv".format(args.location), header=0, skipinitialspace=True)
result_df = result_df.loc[(result_df['response_rate(req/s)'] != 0)]
paxos_df = result_df.loc[(result_df['label'] == 'paxos_')].copy()
opaxos_df = result_df.loc[(result_df['label'] == 'opaxos_shamir')].copy()
fastopaxos_df = result_df.loc[(result_df['label'] == 'fastopaxos_shamir')].copy()

f, (ax1) = plt.subplots(1, 1)
f.set_size_inches(6.4, 2.4)
ax1.set_ylim(top=args.top, bottom=0)
ax1.grid()

data = [paxos_df, opaxos_df, fastopaxos_df]
labels = ['Paxos', 'OPaxos (Shamir)', 'Fast-OPaxos (Shamir)']
colors = ['#1f77b4', '#ff7f0e', '#9F2B68']
markers = ['.', '+', 'x']
capacities = []

for i, raw in enumerate(data):
  raw = raw.sort_values(by=['load(req/s)'])
  x = raw['load(req/s)']
  y = raw['avg_lat(ms)']
  z = raw['load_diff']
  ax1.plot(x, y, label=labels[i], color=colors[i], marker=markers[i])
  capacity = np.interp(load_diff_at_cap, z, x)
  # ax1.axvline(x=capacity, linestyle='--', color=colors[i])
  capacities.append(capacity)

ax1.legend()
ax1.set_ylabel('Avg Latency (ms)')
ax1.set_xlabel('Load (req/s)')
ax1.get_yaxis().set_label_coords(-0.07,0.5)

f.subplots_adjust(wspace=0, hspace=0.05)
f.show()

print('Est.Capacity \t % of Paxos \t Name')
for i, raw in enumerate(data):
  percentage = capacities[i]/capacities[0]*100
  print(f'{capacities[i]:.2f} \t {percentage:.2f}% \t {labels[i]}')

plt.savefig(args.location + "/capacity", bbox_inches='tight')
fig_loc = args.location + "/capacity.png"
print(">> ✅ 🎉 please check the result figure at {}".format(fig_loc))