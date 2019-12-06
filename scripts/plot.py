import matplotlib.pyplot as plt
import numpy as np
import sys


MSEC_TO_SEC = 1000000

def GetQueryName(line):
	return line.split()[2]
		
def GetRunningTime(line):
	running_time = int(line.split()[1])
	return running_time

def GetCompetitiveRatio(line):
	cr = eval(line.split()[1])
	return cr

# return a dictionary: {query_name : a list of running times}
def GetDictionary(data_directories):
	ret = {}
	cr = 1

	isCR = False
	init = False
	for data_directory in data_directories:
		print(data_directory)
		try:
			file = open(data_directory, "r")
		except FileNotFoundError:
			continue
		line = file.readline()[:-1]
		while line != "":
			query_name = GetQueryName(line)
			line = file.readline()[:-1]
			if not init:
				if line.split()[0] == 'CR':
					isCR = True
			
			if isCR:
				cratio = GetCompetitiveRatio(line)
				cr = max(cr, cratio)
				line = file.readline()[:-1]
		
			while True:
				line = file.readline()[:-1]
				running_time = GetRunningTime(line)
				if query_name not in ret:
					ret[query_name] = [1.0 * running_time]
				else:
					ret[query_name].append(1.0 * running_time)
				line = file.readline()[:-1]
				if line == "" or line.split()[0] == "Running":
					break
		file.close()

	return ret, cr



def produce_time_plot(hash_file_base, start, end):
	hash_dict, cr = GetDictionary([hash_file_base + str(i) for i in range(start, end)])
	hash_time = []
	
	for q in hash_dict:
		hash_time.append(min(hash_dict[q]) / MSEC_TO_SEC)
	
	index = [i for i in range(1, len(hash_time)+1)]

	hash_plot = plt.plot(index, hash_time, '-o')

	return cr	

def compute_competitive_ratio(hash_file_base, start, end):
	hash_dict, cr = GetDictionary([hash_file_base + str(i) for i in range(start, end)])
	
	return cr	




def plot_time(start, end):
	directories = [	"date-5-5",
				 	"date-10-10",
				 	"date-20-20", 
				 	"date-50-50",
				 	"date-10-50",
			  		"date-50-10",
				   	"date-linear",
				    "date-linear-part-20-20",
					"date-linear-part-20-20-supp-10-20",
					"date-part-adversary",
					"date-first-half"
					]
	lipK1 = [i for i in range(1, 10)]
	lipK2 = [i for i in range(10, 50, 5)]
	lipK3 = [i for i in range(50, 110, 10)]

	lipK = [1,5,10,20,40,60,80,100]#lipK1 + lipK2 + lipK3 
		

	for directory in directories:
		dir_base = "./scripts/data/" + directory + "/"

		# hash_plot = produce_time_plot(dir_base + "hash_", start, end)
		lip_plot  = produce_time_plot(dir_base + "lip_", start, end)
		
		for i in lipK:
			lip_plot = produce_time_plot(dir_base + "lip-" + str(i) + "_", start, end)
		
		legend_label_list = ['LIP']
		for i in lipK:
			legend_label_list.append('LIP-' + str(i))

		plt.gca().legend(tuple(legend_label_list))
		plt.xlabel("Query #")
		plt.ylabel("Running time (s)")
		plt.show()


def plot_cr(start, end):
	directories = [	"date-5-5",
				 	"date-10-10",
				 	"date-20-20", 
				 	"date-50-50",
				 	"date-10-50",
			  		"date-50-10",
				   	"date-linear",
				    "date-linear-part-20-20",
					"date-linear-part-20-20-supp-10-20",
					"date-part-adversary",
					"date-first-half"
					]

	lipK1 = [i for i in range(1, 10)]
	lipK2 = [i for i in range(10, 50, 5)]
	lipK3 = [i for i in range(50, 110, 10)]

	lipK = lipK1 + lipK2 + lipK3 
	
	overall_cr = {}
	for i in lipK:
		alg_name = "lip" + str(i) 
		overall_cr[alg_name] = 1
	overall_cr["lip"] = 1
	

	for directory in directories:
		dir_base = "./scripts/data/" + directory + "/"

		
		for i in lipK:
			lip_i_cr = compute_competitive_ratio(dir_base + "lip-" + str(i) + "_", start, end)
			alg_name = "lip" + str(i)
			overall_cr[alg_name] = max(overall_cr[alg_name], lip_i_cr)
		
		cr_lip  = compute_competitive_ratio(dir_base + "lip_", start, end)
		
		overall_cr["lip"] = max(overall_cr["lip"], cr_lip)
	
	plt.plot(lipK + [2*max(lipK)], list(overall_cr.values()), '-o')
	plt.xlabel("k")
	plt.ylabel("Comp. ratio ( = ALG #probes/ OPT #probes)")
		
	plt.show()



def main():
	# if len(sys.argv) < 3:
	# 	plot(1, 2)
	# else:
	# 	plot(int(sys.argv[1]), int(sys.argv[2]))
	plot_time(1, 5)
	# plot_cr(1, 2)
	#ret, cr = GetDictionary(["./scripts/data/date-5-5/lip_1"])
	#print(ret, cr)
	
if __name__ == "__main__":
	main()

