import matplotlib.pyplot as plt
import numpy as np


def plot():

	# insert_ht, time_ht = np.loadtxt("../probe-tests/ht_build.dat",delimiter=",", unpack=True, skiprows=1);
	# insert_bf, time_bf = np.loadtxt("../probe-tests/bf_build.dat",delimiter=",", unpack=True, skiprows=1);

	# plt.plot(insert_ht, time_ht, label="Hash Table")
	# plt.plot(insert_bf, time_bf, label="Bloom Filter")
	# plt.ylabel("Duration")
	# plt.xlabel("Number of elements inserted")
	# plt.legend()
	# plt.show()

	# insert_bf_10k, time_bf_10k = np.loadtxt("../probe-tests/bf_probe_10000.dat",delimiter=",", unpack=True, skiprows=1);
	# insert_bf_20k, time_bf_20k = np.loadtxt("../probe-tests/bf_probe_20000.dat",delimiter=",", unpack=True, skiprows=1);
	# insert_bf_30k, time_bf_30k = np.loadtxt("../probe-tests/bf_probe_30000.dat",delimiter=",", unpack=True, skiprows=1);
	# insert_bf_40k, time_bf_40k = np.loadtxt("../probe-tests/bf_probe_40000.dat",delimiter=",", unpack=True, skiprows=1);

	#insert_bf_500, time_bf_500 = np.loadtxt("./probe-tests/bf_probe_500.dat",delimiter=",", unpack=True, skiprows=1);
	insert_bf_1k, time_bf_1k = np.loadtxt("../probe-tests/bf_probe_1k.dat",delimiter=",", unpack=True, skiprows=1);
	insert_bf_5k, time_bf_5k = np.loadtxt("../probe-tests/bf_probe_5k.dat",delimiter=",", unpack=True, skiprows=1);
	insert_bf_10k, time_bf_10k = np.loadtxt("../probe-tests/bf_probe_10k.dat",delimiter=",", unpack=True, skiprows=1);
	insert_bf_20k, time_bf_20k = np.loadtxt("../probe-tests/bf_probe_20k.dat",delimiter=",", unpack=True, skiprows=1);
	insert_bf_50k, time_bf_50k = np.loadtxt("../probe-tests/bf_probe_50k.dat",delimiter=",", unpack=True, skiprows=1);
	insert_bf_100k, time_bf_100k = np.loadtxt("../probe-tests/bf_probe_100k.dat",delimiter=",", unpack=True, skiprows=1);
	insert_bf_500k, time_bf_500k = np.loadtxt("../probe-tests/bf_probe_500k.dat",delimiter=",", unpack=True, skiprows=1);
	insert_bf_1000k, time_bf_1000k = np.loadtxt("../probe-tests/bf_probe_1000k.dat",delimiter=",", unpack=True, skiprows=1);


	# plt.plot(insert_bf_10k, time_bf_10k, label="m = 10k")
	# plt.plot(insert_bf_20k, time_bf_20k, label="m = 20k")
	# plt.plot(insert_bf_30k, time_bf_30k, label="m = 30k")
	# plt.plot(insert_bf_40k, time_bf_40k, label="m = 40k")
	plt.plot(insert_bf_1k, time_bf_1k, label="n = 1k")
	plt.plot(insert_bf_5k, time_bf_5k, label="n = 5k")
	plt.plot(insert_bf_10k, time_bf_10k, label="n = 10k")
	plt.plot(insert_bf_20k, time_bf_20k, label="n = 20k")
	plt.plot(insert_bf_50k, time_bf_50k, label="n = 50k")
	plt.plot(insert_bf_100k, time_bf_100k, label="n = 100k")
	plt.plot(insert_bf_500k, time_bf_500k, label="n = 500k")
	plt.plot(insert_bf_1000k, time_bf_1000k, label="n = 1000k")
	plt.ylabel("Average Duration (over 10 runs)")
	plt.xlabel("Number of elements inserted")
	plt.xscale("log")
	plt.legend()
	plt.show()

	# num_cells1000, num_inserts, time1000 = np.loadtxt("../probe-tests/1000.dat",delimiter=",", unpack=True)
	# num_cells5000, num_inserts, time5000 = np.loadtxt("../probe-tests/5000.dat",delimiter=",", unpack=True)
	# num_cells10000, num_inserts, time10000 = np.loadtxt("../probe-tests/10000.dat",delimiter=",", unpack=True)
	# num_cells20000, num_inserts, time20000 = np.loadtxt("../probe-tests/20000.dat",delimiter=",", unpack=True)
	# num_cells40000, num_inserts, time40000 = np.loadtxt("../probe-tests/40000.dat",delimiter=",", unpack=True)
	# num_cells60000, num_inserts, time60000 = np.loadtxt("../probe-tests/60000.dat",delimiter=",", unpack=True)


	# plt.plot(num_cells1000, time1000, '-', label="1000")
	# plt.plot(num_cells5000, time5000, '-', label="5000")
	# plt.plot(num_cells10000, time10000, '-', label="10000")
	# plt.plot(num_cells20000, time20000, '-', label="20000")
	# plt.plot(num_cells40000, time40000, '-', label="40000")
	# plt.plot(num_cells60000, time60000, '-', label="60000")
	# plt.xscale("log")
	# plt.legend()

	# hash_dict = GetDictionary([hash_file_base])
	# lip_dict = GetDictionary([lip_file_base])
	# xiating_dict = GetDictionary([xiating_file_base])


	# lip_time = []
	# hash_time = []
	# xiating_time = []
	# index = []
	# cnt = 1

	# for q in lip_dict:
	# 	index.append(cnt)
	# 	# lip_time.append(sum(lip_dict[q]) / len(lip_dict[q]))
	# 	lip_time.append(min(lip_dict[q]))
	# 	hash_time.append(min(hash_dict[q]))
	# 	xiating_time.append(min(xiating_dict[q]))

	# 	cnt += 1
	# lip_plot = plt.plot(index, lip_time, '-o')
	# hash_plot = plt.plot(index, hash_time, '-o')
	# xiating_plot = plt.plot(index, xiating_time, '-o')
	
	# plt.gca().legend(('LIP Standard', 'Hash-join', 'Xiating'))

	plt.show()

def main():
	plot()
if __name__ == "__main__":
	main()

