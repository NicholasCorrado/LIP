
import random, os
os.chdir("./scripts")
from dist import *
os.chdir("..")

DIM_NUM = 10


output_dir = "./benchmarks/benchmark-n-dim/"

KEY_RANGE = 10000

def GenerateDim(n, filename, p):
	dim_file = open(output_dir + filename, "w")
	keys = []
	for i in range(n):
		if random.random() < p:
			print(i, file=dim_file)
			keys.append(i)

	dim_file.close()
	return keys


def GenerateFacts(rows):
	output_file = output_dir + "fact.tbl"
	outfile = open(output_file, "w")

	dists = []
	keys = []

	for i in range(DIM_NUM):
		dists.append(RandomDistribution(range(KEY_RANGE)))
		keys.append({})
		

	for i in range(rows):
		if (i % (rows / 100) == 0):
			print(i / (rows / 100), "percent generated.")
		toPrint = []

		for i in range(DIM_NUM):
			value = dists[i].sample()
			if value not in keys[i]:
				keys[i][value] = 1
			else:
				keys[i][value] += 1

			toPrint.append(str(value))


		toPrintLine = "|".join(toPrint)
		print(toPrintLine, file=outfile)
	outfile.close()

	selectivities = [0.8 for i in range(DIM_NUM)]

	for i in range(DIM_NUM):
		keys_i = list(keys[i].keys())
		dim_file_name = output_dir + "dim" + str(i) + ".tbl"

		dim_file = open(dim_file_name,"w")

		cutoff = len(keys_i) * selectivities[i]
		cnt = 0
		for key in keys_i:
			if cnt >= cutoff:
				break

			print(key, file=dim_file)

			cnt += keys[i][key]

		dim_file.close()

def main():
	GenerateFacts(1000000)

	


if __name__ == "__main__":
	main()