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

def get_stdev(hash_file_base, start, end):
	hash_dict, cr = GetDictionary([hash_file_base + str(i) for i in range(start, end)])
	hash_time = []
	
	for q in hash_dict:
		hash_time.append(np.std(hash_dict[q]) / MSEC_TO_SEC)

	hash_time = [hash_time[j] for j in range(6,len(hash_time))]

	return hash_time

def produce_time_plot(hash_file_base, start, end):


	# # set width of bar
	# barWidth = 0.25
	 
	# # set height of bar
	# bars1 = [12, 30, 1, 8, 22]
	# bars2 = [28, 6, 16, 5, 10]
	# bars3 = [29, 3, 24, 25, 17]

	# # Set position of bar on X axis
	# r1 = np.arange(len(bars1))
	# r2 = [x + barWidth for x in r1]
	# r3 = [x + barWidth for x in r2]
	 
	# # Make the plot
	# plt.bar(r1, bars1, color='#7f6d5f', width=barWidth, edgecolor='white', label='var1')
	# plt.bar(r2, bars2, color='#557f2d', width=barWidth, edgecolor='white', label='var2')
	# plt.bar(r3, bars3, color='#2d7f5e', width=barWidth, edgecolor='white', label='var3')
	 
	# # Add xticks on the middle of the group bars
	# plt.xlabel('group', fontweight='bold')
	# plt.xticks([r + barWidth for r in range(len(bars1))], ['A', 'B', 'C', 'D', 'E'])
	 
	# # Create legend & Show graphic
	# plt.legend()
	# plt.show()



	hash_dict, cr = GetDictionary([hash_file_base + str(i) for i in range(start, end)])
	hash_time = []
	
	for q in hash_dict:
		hash_time.append(min(hash_dict[q]) / MSEC_TO_SEC)



	query = ['Q2.1', 'Q3.2', 'Q4.2']
	hash_time = [hash_time[j] for j in range(6,len(hash_time))]

	return hash_time

	# hash_plot = plt.plot(query, hash_time, '-o')





# For the presentation only iterested in Q2.1 (not joining on skew col), Q3.2 (lipk < lip), and Q4.2 (lipk > lip)
# since these queries succintly summarize our results.

def plot_time(start, end):

	PLOT_HASH = False

	PLOT_HASH = False

	directories = [	"date-50-50",
				   	"date-linear",
					"date-part-adversary",
					"date-first-half"
					]

	lipK = [1,10,20,50,80,100]


	for directory in directories:
		dir_base = "./scripts/data/" + directory + "/"

		hash_time = produce_time_plot(dir_base + "hash_", start, end)
		lip_time  = produce_time_plot(dir_base + "lip_", start, end)
		lip1_time = produce_time_plot(dir_base + "lip-1" + "_", start, end)
		lip10_time = produce_time_plot(dir_base + "lip-10" + "_", start, end)
		lip20_time = produce_time_plot(dir_base + "lip-20" + "_", start, end)
		lip50_time = produce_time_plot(dir_base + "lip-50" + "_", start, end)
		lip80_time = produce_time_plot(dir_base + "lip-80" + "_", start, end)
		lip100_time = produce_time_plot(dir_base + "lip-100"  + "_", start, end)

		hash_err = get_stdev(dir_base + "hash_", start, end)
		lip_err  = get_stdev(dir_base + "lip_", start, end)
		lip1_err = get_stdev(dir_base + "lip-1" + "_", start, end)
		lip10_err = get_stdev(dir_base + "lip-10" + "_", start, end)
		lip20_err = get_stdev(dir_base + "lip-20" + "_", start, end)
		lip50_err = get_stdev(dir_base + "lip-50" + "_", start, end)
		lip80_err = get_stdev(dir_base + "lip-80" + "_", start, end)
		lip100_err = get_stdev(dir_base + "lip-100"  + "_", start, end)

		
		legend_label_list = []
		for i in lipK:
			legend_label_list.append('LIP-' + str(i))
		legend_label_list.append('LIP')

		times = [ lip1_time, lip10_time, lip20_time, lip50_time, lip80_time, lip100_time, lip_time]
		if (PLOT_HASH):
			times.append(hash_time)
			legend_label_list.append('Hash')
		# bars1 = [q[0] for q in times]
		# bars2 = [q[1] for q in times]
		# bars3 = [q[2] for q in times]

		# set width of bar
		barWidth = 0.1
		print(times)

		# Set position of bar on X axis
		# rh = np.arange(len(lip_time))
		# rl = [x + barWidth for x in rh]
		# r1 = [x + barWidth for x in rl]
		# r10 = [x + barWidth for x in r1]
		# r20 = [x + barWidth for x in r10]
		# r50 = [x + barWidth for x in r20]
		# r80 = [x + barWidth for x in r50]
		# r100 = [x + barWidth for x in r80]

		r1 = np.arange(len(lip1_time))
		r10 = [x + barWidth for x in r1]
		r20 = [x + barWidth for x in r10]
		r50 = [x + barWidth for x in r20]
		r80 = [x + barWidth for x in r50]
		r100 = [x + barWidth for x in r80]
		rl = [x + barWidth for x in r100]
		rh = [x + barWidth for x in rl]

		# r2 = [x + barWidth for x in r1]
		# r3 = [x + barWidth for x in r2]

		# Make the plot
		plt.figure(num=1, figsize=(8, 7))

		plt.bar(r1, lip1_time,  	yerr=lip1_err, 		color='tab:red', width=barWidth, edgecolor='black')
		plt.bar(r10, lip10_time,  	yerr=lip10_err, 	color='tab:orange', width=barWidth, edgecolor='black')
		plt.bar(r20, lip10_time, 	yerr=lip20_err, 	color='gold',width=barWidth, edgecolor='black')
		plt.bar(r50, lip20_time,	yerr=lip50_err, 	color='tab:green', width=barWidth, edgecolor='black')
		plt.bar(r80, lip50_time, 	yerr=lip80_err, 	color='tab:blue', width=barWidth, edgecolor='black')
		plt.bar(r100, lip80_time, 	yerr=lip100_err, 	color='indigo', width=barWidth, edgecolor='black')
		plt.bar(rl, lip_time,		yerr=lip_err, 		color='tab:purple', width=barWidth, edgecolor='black')
		if (PLOT_HASH): 
			plt.bar(rh, hash_time,  color='grey', width=barWidth, edgecolor='black')
		# plt.bar(r1, bars1, color='#7f6d5f', width=barWidth, edgecolor='white', label='var1')
		# plt.bar(r2, bars2, color='#557f2d', width=barWidth, edgecolor='white', label='var2')
		# plt.bar(r3, bars3, color='#2d7f5e', width=barWidth, edgecolor='white', label='var3')

		 
		# Add xticks on the middle of the group bars
		plt.xlabel('group', fontweight='bold')
		plt.xticks([r + barWidth*3 for r in range(len(lip_time))], ['Q3.1','Q3.2','Q3.3','Q3.4','Q4.1','Q4.2','Q4.3'])
		 
		# Create legend & Show graphic
		plt.gca().legend(tuple(legend_label_list), prop={'size': 12})
		# plt.ylim(0,1.75)
		plt.xlabel("SSB Query")
		plt.ylabel("Running time (s)")


		plt.show()


def plot_time_hash(start, end):

	PLOT_HASH = False

	directories = [	"date-50-50",
				   	"date-linear",
					"date-part-adversary",
					"date-first-half"
					]

	lipK = [1,10,20,50,80,100]


	for directory in directories:
		dir_base = "./scripts/data/" + directory + "/"

		hash_time = produce_time_plot(dir_base + "hash_", start, end)
		lip_time  = produce_time_plot(dir_base + "lip_", start, end)
		lip1_time = produce_time_plot(dir_base + "lip-1" + "_", start, end)
		lip10_time = produce_time_plot(dir_base + "lip-10" + "_", start, end)
		lip20_time = produce_time_plot(dir_base + "lip-20" + "_", start, end)
		lip50_time = produce_time_plot(dir_base + "lip-50" + "_", start, end)
		lip80_time = produce_time_plot(dir_base + "lip-80" + "_", start, end)
		lip100_time = produce_time_plot(dir_base + "lip-100"  + "_", start, end)

		
		legend_label_list = []
		for i in lipK:
			#legend_label_list.append('LIP-' + str(i))
			x = 1
		legend_label_list.append('LIP')

		times = [ lip1_time, lip10_time, lip20_time, lip50_time, lip80_time, lip100_time, lip_time]
		if (PLOT_HASH):
			legend_label_list.append('Hash')
		# bars1 = [q[0] for q in times]
		# bars2 = [q[1] for q in times]
		# bars3 = [q[2] for q in times]

		# set width of bar
		barWidth = 0.1

		# Set position of bar on X axis
		# rh = np.arange(len(lip_time))
		# rl = [x + barWidth for x in rh]
		# r1 = [x + barWidth for x in rl]
		# r10 = [x + barWidth for x in r1]
		# r20 = [x + barWidth for x in r10]
		# r50 = [x + barWidth for x in r20]
		# r80 = [x + barWidth for x in r50]
		# r100 = [x + barWidth for x in r80]

		r1 = np.arange(len(lip1_time))
		r10 = [x + barWidth for x in r1]
		r20 = [x + barWidth for x in r10]
		r50 = [x + barWidth for x in r20]
		r80 = [x + barWidth for x in r50]
		r100 = [x + barWidth for x in r80]
		rl = [x + barWidth for x in r100]
		rh = [x + barWidth for x in rl]

		# r2 = [x + barWidth for x in r1]
		# r3 = [x + barWidth for x in r2]

		# Make the plot
		plt.figure(num=1, figsize=(8, 7))

		# plt.bar(r1, lip1_time,  color='tab:red', width=barWidth, edgecolor='white')
		# plt.bar(r10, lip10_time,  color='tab:orange', width=barWidth, edgecolor='white')
		# plt.bar(r20, lip10_time, color='gold',width=barWidth, edgecolor='white')
		# plt.bar(r50, lip20_time, color='tab:green', width=barWidth, edgecolor='white')
		# plt.bar(r80, lip50_time, color='tab:blue', width=barWidth, edgecolor='white')
		# plt.bar(r100, lip80_time, color='indigo', width=barWidth, edgecolor='white')
		plt.bar(rl, lip_time, color='tab:purple', width=barWidth, edgecolor='white')
		if (PLOT_HASH): 
			plt.bar(rh, hash_time,  color='grey', width=barWidth, edgecolor='white')
		# plt.bar(r1, bars1, color='#7f6d5f', width=barWidth, edgecolor='white', label='var1')
		# plt.bar(r2, bars2, color='#557f2d', width=barWidth, edgecolor='white', label='var2')
		# plt.bar(r3, bars3, color='#2d7f5e', width=barWidth, edgecolor='white', label='var3')

		 
		# Add xticks on the middle of the group bars
		plt.xlabel('group', fontweight='bold')
		plt.xticks([r + barWidth*6.5 for r in range(len(lip_time))], ['Q2.1','Q3.2','Q4.2'])
		 
		# Create legend & Show graphic
		plt.gca().legend(tuple(legend_label_list), prop={'size': 12})
		# plt.ylim(0,1.75)
		plt.xlabel("SSB Query")
		plt.ylabel("Running time (s)")


		plt.show()


def main():

	plot_time(1, 2)
	#plot_time_hash(1,5)
	
if __name__ == "__main__":
	main()

