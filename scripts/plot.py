import matplotlib.pyplot as plt
import numpy as np


def GetQueryName(line):
	return line.split()[2]
		
def GetRunningTime(line):
	running_time = int(line.split()[1])
	return running_time

# return a dictionary: query_name : a list of running times
def GetDictionary(data_directories):
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




RANGE = range(1122000, 1122007)


def produce_plot(hash_file_base):

	hash_dict = GetDictionary([hash_file_base + str(i) for i in RANGE])
	hash_time = []
	
	for q in hash_dict:
		hash_time.append(min(hash_dict[q]))
	index = [i for i in range(1, len(hash_time)+1)]
	hash_plot = plt.plot(index, hash_time, '-o')

	return hash_time	

def plot():
	hash_plot = produce_plot("./scripts/data/hash_enum_")
	lip_plot  = produce_plot("./scripts/data/lip_enum_")
	xiating_plot = produce_plot("./scripts/data/xiating_enum_")
	resur_plot = produce_plot("./scripts/data/resur_enum_")
	lipk_plot = produce_plot("./scripts/data/lipk_enum_")



	# lip_plot = plt.plot(index, lip_time, '-o')
	# xiating_plot = plt.plot(index, xiating_time, '-o')
	# resur_plot = plt.plot(index, resur_time, '-o')
	# lipk_plot = plt.plot(index, lipk_time, '-o')

	plt.gca().legend(('Hash', 'LIP', 'Xiating', 'resur', 'LIPk'))

	plt.show()

def main():
	plot()
if __name__ == "__main__":
	main()

