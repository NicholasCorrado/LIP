import matplotlib.pyplot as plt
import numpy as np
import sys


# return a dictionary: {query_name : a list of running times}
def GetCR(filename):
	cr = 1

	isCR = False
	init = False
	print(filename)
	try:
		file = open(filename, "r")
	except FileNotFoundError:
		print("File not found.")
		return
	line = file.readline()[:-1]
	while line != "":
		print(line)
		line = file.readline()[:-1]
		if line.startswith("CR"):
			cr = float(line.split()[1])
	file.close()

	return cr


def plot_cr(start, end):


	lipK1 = [i for i in range(1, 10)]
	lipK2 = [i for i in range(10, 100, 5)]
	lipK3 = [i for i in range(100, 500, 50)]

	lipK = lipK1 + lipK2 + lipK3 + [562]
	
	all_cr = []
	for i in lipK:
		all_cr.append( GetCR("./scripts/data/date-part-adversary-CR/lip-" + str(i) + "_CR") )
	
	X = lipK
	Y = all_cr
	print(X)
	print(Y)
	plt.plot(X, Y, '-o')
	plt.xlabel("k")
	plt.ylabel("Comp. ratio ( = ALG #probes/ OPT #probes)")
		
	plt.show()



def main():
	# if len(sys.argv) < 3:
	# 	plot(1, 2)
	# else:
	# 	plot(int(sys.argv[1]), int(sys.argv[2]))
	#plot_time(1, 5)
	plot_cr(1, 2)
	#ret, cr = GetDictionary(["./scripts/data/date-5-5/lip_1"])
	#print(ret, cr)
	
if __name__ == "__main__":
	main()

