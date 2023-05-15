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
opaxos_ssms_df = result_df.loc[(result_df['label'] == 'opaxos_ssms')].copy()

f, (ax1, ax2) = plt.subplots(2, 1, sharex=True)
ax1.set_ylim(top=args.top, bottom=0)
ax1.grid()

data = [paxos_df, opaxos_df, opaxos_ssms_df]
labels = ['Paxos', 'OPaxos (Shamir)', 'OPaxos (SSMS)']
colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
markers = ['.', '+', 'x']
capacities = []

for i, raw in enumerate(data):
  raw = raw.sort_values(by=['load(req/s)'])
  x = raw['load(req/s)']
  y = raw['median_lat(ms)']
  z = raw['load_diff']
  ax1.plot(x, y, label=labels[i], color=colors[i], marker=markers[i])
  capacity = np.interp(load_diff_at_cap, z, x)
  ax1.axvline(x=capacity, linestyle='--', color=colors[i])
  capacities.append(capacity)

ax1.legend()
ax1.set_ylabel('Median Latency (ms)')
ax1.get_yaxis().set_label_coords(-0.07,0.5)

for i, raw in enumerate(data):
  raw = raw.sort_values(by=['load(req/s)'])
  x = raw['load(req/s)']
  y = raw['response_rate(req/s)']/1000
  ax2.plot(x, y, label=labels[i], color=colors[i], marker=markers[i])
  ax2.axvline(x=capacities[i], linestyle='--', color=colors[i])

ax2.plot(np.arange(55000), np.arange(55000)/1000, zorder=1, color='gray', linestyle=':')

ax2.grid()
ax2.set_ylabel('Resp Rate (K req/s)')
ax2.get_yaxis().set_label_coords(-0.07,0.5)
ax2.set_xlabel('Load (req/s)')
ax2.set_ylim(bottom=0, top=args.top2)
ax1.set_xlim(left=0, right=args.right)

f.subplots_adjust(wspace=0, hspace=0.05)
f.show()

for i, raw in enumerate(data):
  percentage = capacities[i]/capacities[0]*100
  print(f'{capacities[i]} \t {percentage:.2f}% \t {labels[i]}')

plt.savefig(args.location + "/capacity", bbox_inches='tight')
fig_loc = args.location + "/capacity.png"
print("please check the result in {}".format(fig_loc))