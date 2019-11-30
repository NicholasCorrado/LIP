
import random, os
#os.chdir("./scripts")
from dist import *
#os.chdir("..")
input_dir = "./benchmarks/benchmark-1/"
output_dir = "./benchmarks/benchmark-skew/"

FACT_SCHEMA = ["ORDER KEY", "LINE NUMBER", "CUST KEY", "PART KEY", "SUPP KEY",
               "ORDER DATE", "ORD PRIORITY", "SHIP PRIORITY", "QUANTITY",
               "EXTENDED PRICE", "ORD TOTAL PRICE", "DISCOUNT", "REVENUE",
               "SUPPLY COST", "TAX", "COMMIT DATE", "SHIP MODE"];

FIELD = {}
index = 0

NUM_BATCHES = 562 # number of batches in fact table with SF = 1 and default chunksize



# create dictionary mapping column names to column indices
for field in FACT_SCHEMA:
	FIELD[field] = index
	index += 1


def loadKeys():

	custKeyList = []
	partKeyList = []
	suppKeyList = []
	dateKeyList = []

	years = [i for i in range(1992, 1998+1)]
	months = [i for i in range(1, 12+1)]
	days = [i for i in range(1,31+1)]


	i = 0
	for year in years:
		for month in months:
			for day in days:

				key = str(year)

				if month < 10:
					key += "0" + str(month)
				else:
					key += str(month)


				if day == 31 and month not in [1,3,5,7,8,10,12]:
					#key += str(day)
					continue
				elif year in [1992, 1996] and month == 2 and day == 29:
					key += str(day)
				elif month == 2 and day > 28:
					continue
				elif day < 10:
					key += "0" + str(day)
				else:
					key += str(day)

				dateKeyList.append(key)

	custKeyList = [str(i) for i in range(1, 30000+1)]
	partKeyList = [str(i) for i in range(1, 200000+1)]
	suppKeyList = [str(i) for i in range(1, 2000+1)]

	return custKeyList, partKeyList, suppKeyList, dateKeyList

def GenerateRows():

	custKeyList, partKeyList, suppKeyList, dateKeyList = loadKeys()

	table_file = input_dir + "lineorder.tbl"
	
	infile = open(table_file, "r")

	output_file = output_dir + "lineorder-1997-1998-20-20.tbl"
	outfile = open(output_file, "w")

	line = infile.readline()[:-1]
	sampleLine = line.split("|")
	lineno = 1



	custDist = UniformDistribution(custKeyList)
	partDist = UniformDistribution(partKeyList)
	suppDist = UniformDistribution(suppKeyList)
	dateDist = UniformDistribution(dateKeyList)

	print("# Custkey:", len(custKeyList))
	print("# Partkey:", len(partKeyList))
	print("# Suppkey:", len(suppKeyList))
	print("# Datekey:", len(dateKeyList))

	# dim_file = open(input_dir + "part.tbl")
	# line = dim_file.readline()[:-1]

	# good_keys = []
	# line = dim_file.readline()[:-1]
	# while line != "":
			
	# 	debris = line.split("|"	)

	# 	if debris[2] == "MFGR#1" or debris[2] == "MFGR#2":
	# 		good_keys.append(debris[0])

	# 	line = dim_file.readline()[:-1]

	# dim_file.close()

	# Here, we can control which part of the distribution from which we want to generate tuples
	# We can even sample from different distributions over time!
	for batch_num in range(NUM_BATCHES):
		print("Writing batch", batch_num, "...")
		for i in range(6000000//NUM_BATCHES):
			line = infile.readline()[:-1]
			debris = line.split("|")

			# odd batches have no 1993 keys
			# even batcheas have only 1993 keys
			if batch_num % 20 < 10:
				debris[5] = "19970101"
				print("1997")
			elif (debris[5].startswith("1997") or debris[5].startswith("1998")):
				debris[5] = "19920101"
				print("else")

			# if batch_num < NUM_BATCHES//2:
			# 	debris[5] = "19970101"
			# elif (debris[5].startswith("1997") or debris[5].startswith("1998")):
			# 	debris[5] = "19920101"
			

			# if batch_num % 4 != 3 and (debris[3] in good_keys):
			# 	debris[3] = "-1"
			# if batch_num % 4 == 3:
			# 	debris[3] = "1"

			toPrintLine = "|".join(debris)
			print(toPrintLine, file=outfile)

	infile.close()
	outfile.close()


	# for k in range(NUM_BATCHES):
	# 	print("Writing batch ", k, "...")
	# 	for i in range(6000000//NUM_BATCHES):
	# 		toPrint = sampleLine

	# 		# toPrint[FIELD["CUST KEY"]] = custDist.sample(i, rows)
	# 		# toPrint[FIELD["PART KEY"]] = partDist.sample(i, rows)
	# 		# toPrint[FIELD["SUPP KEY"]] = suppDist.sample(i, rows)
	# 		# toPrint[FIELD["ORDER DATE"]] = dateDist.sample(i, rows)

	# 		toPrint[FIELD["CUST KEY"]] = custDist.sample()
	# 		toPrint[FIELD["PART KEY"]] = partDist.sample()
	# 		toPrint[FIELD["SUPP KEY"]] = suppDist.sample()
	# 		toPrint[FIELD["ORDER DATE"]] = dateDist.sample()

	# 		toPrintLine = "|".join(toPrint)
	# 		print(toPrintLine, file=outfile)

	# 	# UPDATE DISTRIBUTIONS HERE
	# 	for i in range(len(dateDist.dist)):
	# 		if dateKeyList[i].startswith("1993"):

	# 			if (k % 20 < 10):
	# 				dateDist.dist[i] += 0.1/365
	# 			else:
	# 				dateDist.dist[i] /= 2 
	# 	dateDist.normalize()
	# 	dateDist.computeAccu()

	# outfile.close()


"""
Update a distribution dist such that the values at the inputted indices
increase linearly (while values at all other indices decrease linearly).

It is assumed that the distribution will be updated after every batch.
"""
def updateDistLinear(dist, indices):

	for i in range(len(dist)):

		if i in (i): 
			dist[i] += 1/NUM_BATCHES/(len(indices))*(len(dist)-len(indices))
		else:
			dist[i] -= 1/NUM_BATCHES
			if dist[i] <= 0: dist[i] = 0

	return dist


def main():
	GenerateRows()
	


if __name__ == "__main__":
	main()