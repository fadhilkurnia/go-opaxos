#!/usr/bin/python3
import argparse
import matplotlib.pyplot as plt
import numpy as np
import os

parser = argparse.ArgumentParser(description='experiments script')
parser.add_argument('-l', "--location", type=str, required=True)
args = parser.parse_args()

# make sure the 4 variables below are similar with the variable in the run_lat_eval.sh
experiments = [ ["paxos", "cloudlab_lat_lowload_paxos.json", "Multi-Paxos", ""],
                ["paxos", "cloudlab_lat_lowload_paxos_encrypt.json", "Multi-Paxos (Encrypted)", "encrypt"],
                ["opaxos", "cloudlab_lat_lowload_opaxos.json", "Multi-OPaxos (Shamir)", "shamir"],
                ["opaxos", "cloudlab_lat_lowload_opaxos_ssms.json", "Multi-OPaxos (SSMS)", "ssms"]]
cmd_sizes = [ 50, 100, 1000, 10000, 25000, 50000, 100000 ]
cmd_size_labels = [ '50B', '100B', '1KB', '10KB', '25KB', '50KB', '100KB']

def generate_plot(output_loc):
    plt.rcParams["figure.figsize"] = (5,3)
    # plt.rcParams['font.size'] = 15
    x_labels = cmd_size_labels
    n_labels = len(x_labels)
    width = 0.2

    colors = ['#1f77b4', 'darkturquoise', '#ff7f0e', '#2ca02c', '#d62728']

    for i, e in enumerate(experiments):
        lats = []
        encs = []
        stderrs = []
        stddevs = []
        enc_stddevs = []
        config = "{}_{}.json".format(e[0], e[3])
        label = e[2]

        for sz in cmd_sizes:
            lat_filename = output_loc + "/latency_" + config.replace(".json", "_{}.dat".format(sz))
            enc_filename = output_loc + "/encode_" + config.replace(".json", "_{}.dat".format(sz))
            lat = [0]
            enc = [0]
            if os.path.exists(lat_filename):
                lat = np.loadtxt(lat_filename)
            if os.path.exists(enc_filename):
                enc = np.loadtxt(enc_filename)

            lats.append(np.mean(lat))
            encs.append(np.mean(enc))
            stddevs.append(np.std(lat))
            stderrs.append(np.std(lat)/np.sqrt(np.size(lats)))
            enc_stddevs.append(np.std(enc))

        x_pos = np.arange(len(x_labels))

        avg_bar = plt.bar(x_pos + (i*width) - (width*len(experiments)/2) + width/2,
                lats,
                yerr=[
                      np.zeros(n_labels),
                      stddevs,
                ],
                capsize=3,
                edgecolor='black', color=colors[i], zorder=3, width=width, label=label)
        enc_label = None
        if i == len(experiments)-1:
            enc_label = 'Secret-sharing/Encryption'
        plt.bar(x_pos + (i*width) - (width*len(experiments)/2) + width/2,
                encs,
                bottom=np.subtract(lats, encs),
                hatch='///', color='white', alpha=.90, edgecolor='black', zorder=3, width=width, label=enc_label)
        plt.errorbar(x_pos + (i*width) - (width*len(experiments)/2) + width/2,
                     np.subtract(lats, encs),
                     yerr=enc_stddevs,
                     color='black',
                     fmt='.',
                     markersize=0.01,
                     elinewidth=1,
                     zorder=4)

    plt.ylim(bottom=15, top=17)
    plt.legend(loc='upper left', bbox_to_anchor=(1.05, 1.0))
    plt.ylabel("Average Latency (ms)")
    plt.xlabel("Command Size")
    plt.xticks(x_pos, x_labels)
    plt.grid(zorder=0)
    plt.title("Latency Overhead of Privacy-Preserving Consensus")

    # plt.annotate('small overhead\nfor secret-sharing', xy=(0, 0.4), xytext=(0, 1), arrowprops={"arrowstyle":"->", "color":"gray"})
    # plt.annotate('', xy=(1, 0.4), xytext=(1, 0.9), arrowprops={"arrowstyle":"->", "color":"gray"})
    # plt.annotate('', xy=(2, 0.5), xytext=(1.2, 0.9), arrowprops={"arrowstyle":"->", "color":"gray"})

    # plt.annotate('', xy=(3.3, 1.1), xytext=(3.4, 3.85), arrowprops={"arrowstyle":"->", "color":"gray"})
    # plt.annotate('similar or lower\nlatency with SSMS', xy=(4.3, 1.5), xytext=(3.05, 4), arrowprops={"arrowstyle":"->", "color":"gray"})
    # plt.annotate('', xy=(5.3, 2.1), xytext=(5.2, 3.8), arrowprops={"arrowstyle":"->", "color":"gray"})

    plt.savefig(output_loc + "/latency", bbox_inches='tight')
    
    fig_loc = output_loc + "/latency.png"
    print("please check the result in {}".format(fig_loc))

    return

generate_plot(args.location)