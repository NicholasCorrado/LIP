import matplotlib.pyplot as plt
import numpy as np

# return a dictionary: query_name : running time
def GetDictionary(data_directories):
	ret = {}
	count = {}
	for data_directory in data_directories:
		file = open(data_directory, "r")

		line = file.readline()[:-1]
		while line != "":
			query_name = line.split()[2]
			
			line = file.readline()
			line = file.readline()[:-1]

			running_time = int(line.split()[1])

			if query_name not in ret or \
				query_name not in count:
				ret[query_name] = 1.0 * running_time
				count[query_name] = 1
			else:
				ret[query_name] = 1.0* (ret[query_name] * count[query_name] + running_time) \
									/ (count[query_name] + 1)
				count[query_name] += 1

			line = file.readline()	
	return ret

def main():
	hash_file = "./scripts/data/hash"
	lip_file = "./scripts/data/lip"
	xiating_file = "./scripts/data/xiating"


	lip_dict = GetDictionary([lip_file])
	hash_dict = GetDictionary([hash_file])
	xiating_dict = GetDictionary([xiating_file])


	lip_time = []
	hash_time = []
	xiating_time = []
	index = []
	cnt = 0

	for q in lip_dict:
		index.append(cnt)
		lip_time.append(lip_dict[q])
		hash_time.append(hash_dict[q])
		xiating_time.append(xiating_dict[q])

		cnt += 1
	lip_plot = plt.plot(index, lip_time, '-o')
	hash_plot = plt.plot(index, hash_time, '-o')
	xiating_plot = plt.plot(index, xiating_time, '-o')
	
	plt.gca().legend(('LIP Standard', 'Hash-join', 'Xiating'))

	plt.show()

if __name__ == "__main__":
	main()

