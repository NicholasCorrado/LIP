import matplotlib.pyplot as plt
import numpy as np
import sys

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




def produce_plot(hash_file_base, start, end):

	hash_dict = GetDictionary([hash_file_base + str(i) for i in range(start, end)])
	hash_time = []
	
	for q in hash_dict:
		hash_time.append(min(hash_dict[q]))
	index = [i for i in range(1, len(hash_time)+1)]
	hash_plot = plt.plot(index, hash_time, '-o')

	return hash_time	

def plot(start, end):
	lip_plot  = produce_plot("./scripts/data/date-1-1/lip_enum_", start, end)
	lipk_plot = produce_plot("./scripts/data/date-1-1/lipk_enum_", start, end)

	plt.gca().legend(('LIP', 'LIPk'))
	plt.show()

	lip_plot  = produce_plot("./scripts/data/date-1-2/lip_enum_", start, end)
	lipk_plot = produce_plot("./scripts/data/date-1-2/lipk_enum_", start, end)

	plt.gca().legend(('LIP', 'LIPk'))
	plt.show()	

	lip_plot  = produce_plot("./scripts/data/date-2-1/lip_enum_", start, end)
	lipk_plot = produce_plot("./scripts/data/date-2-1/lipk_enum_", start, end)

	plt.gca().legend(('LIP', 'LIPk'))
	plt.show()	

	lip_plot  = produce_plot("./scripts/data/date-2-2/lip_enum_", start, end)
	lipk_plot = produce_plot("./scripts/data/date-2-2/lipk_enum_", start, end)

	plt.gca().legend(('LIP', 'LIPk'))
	plt.show()	

	lip_plot  = produce_plot("./scripts/data/date-linear/lip_enum_", start, end)
	lipk_plot = produce_plot("./scripts/data/date-linear/lipk_enum_", start, end)

	plt.gca().legend(('LIP', 'LIPk'))
	plt.show()

def main():
	if len(sys.argv) < 3:
		plot(1, 2)
	else:
		plot(int(sys.argv[1]), int(sys.argv[2]))
if __name__ == "__main__":
	main()

