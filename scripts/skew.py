
import random, os
#os.chdir("./scripts")
from dist import *
os.chdir("..")
input_dir = "./benchmarks/benchmark-5/"
output_dir = "./benchmarks/benchmark-skew/"

FACT_SCHEMA = ["ORDER KEY", "LINE NUMBER", "CUST KEY", "PART KEY", "SUPP KEY",
               "ORDER DATE", "ORD PRIORITY", "SHIP PRIORITY", "QUANTITY",
               "EXTENDED PRICE", "ORD TOTAL PRICE", "DISCOUNT", "REVENUE",
               "SUPPLY COST", "TAX", "COMMIT DATE", "SHIP MODE"];

FIELD = {}
index = 0

# create dictionary mapping column names to column indices
for field in FACT_SCHEMA:
	FIELD[field] = index
	index += 1

def GenerateRows(rows):
	table_file = input_dir + "lineorder.tbl"
	
	infile = open(table_file, "r")

	custKeys = {}
	partKeys = {}
	suppKeys = {}
	dateKeys = {}

	line = infile.readline()[:-1]
	sampleLine = line.split("|")
	lineno = 1

	infile.close()

	# This loop grabs all possible keys for each dimension table
	# while line != "":
	# 	debris = line.split("|")
	# 	if (lineno % 100000 == 0):
	# 		print(lineno, "processed.")

	# 	# Grab the FK values of the fact table corresponding to the PK of the dimension tables
	# 	custKey = debris[FIELD["CUST KEY"]]
	# 	partKey = debris[FIELD["PART KEY"]]
	# 	suppKey = debris[FIELD["SUPP KEY"]]
	# 	dateKey = debris[FIELD["ORDER DATE"]]

	# 	if(custKey == "19980803"): 
	# 		print(line)

	# 	if custKey not in custKeys:
	# 		custKeys[custKey] = 1

	# 	if partKey not in partKeys:
	# 		partKeys[partKey] = 1

	# 	if suppKey not in suppKeys:
	# 		suppKeys[suppKey] = 1

	# 	if dateKey not in dateKeys:
	# 		dateKeys[dateKey] = 1

	# 	lineno += 1
	# 	line = infile.readline()[:-1]

	# infile.close()

	# print(sorted(dateKeys.keys()))

	years = [i for i in range(1992, 1998+1)]
	months = [i for i in range(1, 12+1)]
	days = [i for i in range(1,31+1)]

	dateKeyList = [] #7 years, 2 of which are leap years.

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



	
	output_file = output_dir + "lineorder.tbl"
	outfile = open(output_file, "w")

	custKeyList = [str(i) for i in range(1, 30000+1)]
	partKeyList = [str(i) for i in range(1, 200000+1)]
	suppKeyList = [str(i) for i in range(1, 2000+1)]
	#dateKeyList = list(dateKeys.keys())

	# custDist = RandomDistribution(custKeyList)
	# partDist = SquareDistribution(partKeyList)
	# suppDist = RandomDistribution(suppKeyList)
	# dateDist = UniformDistribution(dateKeyList)

	custDist = UniformDistribution(custKeyList)
	partDist = UniformDistribution(partKeyList)
	suppDist = UniformDistribution(suppKeyList)
	dateDist = UniformDistribution(dateKeyList)

	print("# Custkey:", len(custKeyList))
	print("# Partkey:", len(partKeyList))
	print("# Suppkey:", len(suppKeyList))
	print("# Datekey:", len(dateKeyList))

	# Here, we can control which part of the distribution from which we want to generate tuples
	# We can even sample from different distributions over time!
	for k in range(562//2):
		print(k)
		for i in range(rows):
			if (i % (rows / 100) == 0):
				print(i / (rows / 100), "printed.")
			toPrint = sampleLine

			# toPrint[FIELD["CUST KEY"]] = custDist.sample(i, rows)
			# toPrint[FIELD["PART KEY"]] = partDist.sample(i, rows)
			# toPrint[FIELD["SUPP KEY"]] = suppDist.sample(i, rows)
			# toPrint[FIELD["ORDER DATE"]] = dateDist.sample(i, rows)

			toPrint[FIELD["CUST KEY"]] = custDist.sample()
			toPrint[FIELD["PART KEY"]] = partDist.sample()
			toPrint[FIELD["SUPP KEY"]] = suppDist.sample()
			toPrint[FIELD["ORDER DATE"]] = dateDist.sample(i, rows)

			toPrintLine = "|".join(toPrint)
			print(toPrintLine, file=outfile)

	outfile.close()


def main():
	GenerateRows(6000000//562*2)

	


if __name__ == "__main__":
	main()