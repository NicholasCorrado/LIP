import matplotlib.pyplot as plt
import numpy as np



# return a dictionary: query_name : a list of running times
def GetData(data_directories):
	ret = {}
	for data_directory in data_directories:
		file = open(data_directory, "r")

		line = file.readline()[:-1]
		while line != "":
			query_name = GetQueryName(line)
			line = file.readline()[:-1]
			while True:
				line = file.readline()[:-1]
				running_time = GetRunningTime(line)
				if query_name not in ret:
					ret[query_name] = [1.0 * running_time]
				else:
					ret[query_name].append(1.0 * running_time)
				line = file.readline()	
				
				if line == "" or line.split()[0] == "Running":
					break
		file.close()

	return ret

def plot():
	num_cells1000, num_inserts, time1000 = np.loadtxt("../bloom_filter_tests/1000.dat",delimiter=",", unpack=True)
	num_cells5000, num_inserts, time5000 = np.loadtxt("../bloom_filter_tests/5000.dat",delimiter=",", unpack=True)
	num_cells10000, num_inserts, time10000 = np.loadtxt("../bloom_filter_tests/10000.dat",delimiter=",", unpack=True)
	num_cells20000, num_inserts, time20000 = np.loadtxt("../bloom_filter_tests/20000.dat",delimiter=",", unpack=True)
	num_cells40000, num_inserts, time40000 = np.loadtxt("../bloom_filter_tests/40000.dat",delimiter=",", unpack=True)
	num_cells60000, num_inserts, time60000 = np.loadtxt("../bloom_filter_tests/60000.dat",delimiter=",", unpack=True)


	plt.plot(num_cells1000, time1000, '-', label="1000")
	plt.plot(num_cells5000, time5000, '-', label="5000")
	plt.plot(num_cells10000, time10000, '-', label="10000")
	plt.plot(num_cells20000, time20000, '-', label="20000")
	plt.plot(num_cells40000, time40000, '-', label="40000")
	plt.plot(num_cells60000, time60000, '-', label="60000")
	plt.xscale("log")
	plt.legend()

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

