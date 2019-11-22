
import random, os
os.chdir("./scripts")
from dist import *
os.chdir("..")
input_dir = "./benchmarks/benchmark-original/"
output_dir = "./benchmarks/benchmark-skew/"

FACT_SCHEMA = ["ORDER KEY", "LINE NUMBER", "CUST KEY", "PART KEY", "SUPP KEY",
               "ORDER DATE", "ORD PRIORITY", "SHIP PRIORITY", "QUANTITY",
               "EXTENDED PRICE", "ORD TOTAL PRICE", "DISCOUNT", "REVENUE",
               "SUPPLY COST", "TAX", "COMMIT DATE", "SHIP MODE"];

FIELD = {}
index = 0

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
	while line != "":
		debris = line.split("|")
		if (lineno % 100000 == 0):
			print(lineno, "processed.")
		custKey = debris[FIELD["CUST KEY"]]
		partKey = debris[FIELD["PART KEY"]]
		suppKey = debris[FIELD["SUPP KEY"]]
		dateKey = debris[FIELD["ORDER DATE"]]

		if custKey not in custKeys:
			custKeys[custKey] = 1

		if partKey not in partKeys:
			partKeys[partKey] = 1

		if suppKey not in suppKeys:
			suppKeys[suppKey] = 1

		if dateKey not in dateKeys:
			dateKeys[dateKey] = 1

		lineno += 1
		line = infile.readline()[:-1]

	infile.close()
	
	output_file = output_dir + "lineorder.tbl"
	outfile = open(output_file, "w")

	custKeyList = list(custKeys.keys())
	partKeyList = list(partKeys.keys())
	suppKeyList = list(suppKeys.keys())
	dateKeyList = list(dateKeys.keys())


	custDist = RandomDistribution(custKeyList)
	partDist = SquareDistribution(partKeyList)
	suppDist = RandomDistribution(suppKeyList)
	dateDist = UniformDistribution(dateKeyList)

	print("# Custkey:", len(custKeyList))
	print("# Partkey:", len(partKeyList))
	print("# Suppkey:", len(suppKeyList))
	print("# Datekey:", len(dateKeyList))

	for i in range(rows):
		if (i % (rows / 100) == 0):
			print(i / (rows / 100), "printed.")
		toPrint = sampleLine

		toPrint[FIELD["CUST KEY"]] = custDist.sample(i, rows)
		toPrint[FIELD["PART KEY"]] = partDist.sample(i, rows)
		toPrint[FIELD["SUPP KEY"]] = suppDist.sample(i, rows)
		toPrint[FIELD["ORDER DATE"]] = dateDist.sample(i, rows)

		toPrintLine = "|".join(toPrint)
		print(toPrintLine, file=outfile)
	outfile.close()


def main():
	GenerateRows(1000000)

	


if __name__ == "__main__":
	main()