
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

#5999938	4	8450	58648	761	19970127	3-MEDIUM	0	1	160664	23230089	5	152630	96398	4	19970306	FOB	


# create dictionary mapping column names to column indices
for field in FACT_SCHEMA:
	FIELD[field] = index
	index += 1

def GetNumberOfBatches(filename):
	file = open(filename, "r")
	num_batches = 0
	while (file.readline()):
		num_batches += 1
	return num_batches


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

# for i in range(len(dateKeyList)):
# 	if dateKeyList[i].startswith("1997"):
# 		print(i)

# return custKeyList, partKeyList, suppKeyList, dateKeyList




def date_first_half(batch_num, i, num_batches, debris):

	if batch_num < num_batches//2:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
	elif (debris[5].startswith("1997") or debris[5].startswith("1998")):
		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])


def date_linear(batch_num, i, num_batches, debris):

	if (debris[5].startswith("1997") or debris[5].startswith("1998")):
		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])
	if random.random() < batch_num/num_batches:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])

# convention: first digit = low selectivity batch
def date_1_1(batch_num, i, num_batches, debris):

	
	if batch_num % 2 == 0:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
	elif (debris[5].startswith("1997") or debris[5].startswith("1998")):
		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])

def date_2_1(batch_num, i, num_batches, debris):

	

	if batch_num % 3 < 2:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
	elif (debris[5].startswith("1997") or debris[5].startswith("1998")):
		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])

	

def date_1_2(batch_num, i, num_batches, debris):

	if batch_num % 3 < 1:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
	elif (debris[5].startswith("1997") or debris[5].startswith("1998")):
		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])

	

def date_2_2(batch_num, i, num_batches, debris):

	if batch_num % 4 < 2:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
	elif (debris[5].startswith("1997") or debris[5].startswith("1998")):

		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])


def date_5_5(batch_num, i, num_batches, debris):

	if batch_num % 10 < 5:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
	elif (debris[5].startswith("1997") or debris[5].startswith("1998")):

		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])
	
def date_10_10(batch_num, i, num_batches, debris):

	if batch_num % 20 < 10:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
	elif (debris[5].startswith("1997") or debris[5].startswith("1998")):

		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])

def date_20_20(batch_num, i, num_batches, debris):

	if batch_num % 40 < 20:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
	elif (debris[5].startswith("1997") or debris[5].startswith("1998")):

		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])


def date_50_50(batch_num, i, num_batches, debris):

	if batch_num % 100 < 50:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
	elif (debris[5].startswith("1997") or debris[5].startswith("1998")):

		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])

def date_50_10(batch_num, i, num_batches, debris):

	if batch_num % 60 < 50:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
	elif (debris[5].startswith("1997") or debris[5].startswith("1998")):

		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])

def date_10_50(batch_num, i, num_batches, debris):

	if batch_num % 60 < 10:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
	elif (debris[5].startswith("1997") or debris[5].startswith("1998")):
		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])

def date_25_25(batch_num, i, num_batches, debris):

	if batch_num % 50 < 25:
		debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
	elif (debris[5].startswith("1997") or debris[5].startswith("1998")):

		debris[5] = str(dateKeyList[random.randint(0, 1827-1)])


def cust_10_10(batch_num, i, num_batches, debris):
	

	field = FIELD["CUST KEY"]

	if batch_num % 20 == 10:
		debris[field] = "8450"
	else:
		debris[field] = "-1"

	

def part_20_20(batch_num, i, num_batches, debris):
	

	field = FIELD["PART KEY"]

	if batch_num % 40 < 20:
		debris[field] = "1"
	else:
		debris[field] = "-1"

	

def supp_10_20(batch_num, i, num_batches, debris):
	

	field = FIELD["SUPP KEY"]

	if batch_num % 30 < 10:
		debris[field] = "761"
	else:
		debris[field] = "-1"

	

def adversary(batch_num, i, num_batches, debris):


	if batch_num == 0: 
		# date selectivity is 2/5 for the first batch
		if i < 6000000//num_batches*0.4:
			debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
			# datepass += 1
		else:
			debris[5] = str(dateKeyList[random.randint(0, 1827-1)])
		# 3/5 of the tuples that pass the date selection should pass the part selection,
		# but the overall part selectivity for the batch should be less than the date selectivity.
		if i < 6000000//num_batches*0.4*0.6:
			debris[3] = "1"
			# partpass += 1
		else:
			debris[3] = "-1"


	# date selectivity = 1 on even batches
	# part selectivity = 0 on even batches
	else:
		if batch_num % 2 == 1:
			debris[5] = str(dateKeyList[random.randint(1827, 2257-1)])
			debris[3] = "-1"
		# date selectivity = 0 on odd batches (after first batch)
		else:
			debris[5] = str(dateKeyList[random.randint(0, 1827-1)])
			debris[3] = "1"

	debris[2] = "8450"
	debris[4] = "761"



def randomCustKLey(line):

	debris = line.split("|")

	field = FIELD["CUST KEY"]

	debris[field] = custKeyList	


def GenerateRows(out_filename, batch_size_filename=""):

	# Load valid keys
	# custKeyList, partKeyList, suppKeyList, dateKeyList = loadKeys()

	# Inout lineorder table from SF = 1 data set
	table_file = input_dir + "lineorder.tbl"
	infile = open(table_file, "r")

	# output file
	output_file = output_dir + out_filename
	outfile = open(output_file, "w")

	# sample line
	line = infile.readline()[:-1]
	sampleLine = line.split("|")
	lineno = 1

	# Get batch size info
	num_batches = GetNumberOfBatches("./scripts/" + batch_size_filename);
	batch_size_file = open("./scripts/" + batch_size_filename, "r")
	batch_size = batch_size_file.readline()[:-1]

	# Where the magic happens
	batch_num = 0
	while (batch_size):

		# if (batch_num%num_batches == 10):
		print("Writing batch",batch_num,"...")
		batch_size = int(batch_size)

		for row in range(batch_size):

			line = infile.readline()[:-1]
			debris = line.split("|")
			# Place your function to change the current line here
			# toPrintLine = date_linear(batch_num, row, num_batches, line)
			# toPrintLine = adversary(batch_num, row, num_batches, line)
			if (out_filename == "lineorder-date-part-adversary.tbl"):
				adversary(batch_num, row, num_batches, debris)
				#debris[2] = str(random.randint(1,30000))
				#debris[4] = str(random.randint(1,2000))

			elif (out_filename == "lineorder-date-first-half.tbl"):
				date_first_half(batch_num, row, num_batches, debris)



			elif (out_filename == "lineorder-date-1-1.tbl"):
				date_1_1(batch_num, row, num_batches, debris)
				# debris[2] = str(random.randint(1,30000))
				# debris[3] = str(random.randint(1,200000))
				# debris[4] = str(random.randint(1,2000))

			elif (out_filename == "lineorder-date-2-1.tbl"):
				date_2_1(batch_num, row, num_batches, debris)
				# debris[2] = str(random.randint(1,30000))
				# debris[3] = str(random.randint(1,200000))
				# debris[4] = str(random.randint(1,2000))


			elif (out_filename == "lineorder-date-1-2.tbl"):
				date_1_2(batch_num, row, num_batches, debris)
				# debris[2] = str(random.randint(1,30000))
				# debris[3] = str(random.randint(1,200000))
				# debris[4] = str(random.randint(1,2000))

			elif (out_filename == "lineorder-date-2-2.tbl"):
				date_2_2(batch_num, row, num_batches, debris)
				# debris[2] = str(random.randint(1,30000))
				# debris[3] = str(random.randint(1,200000))
				# debris[4] = str(random.randint(1,2000))

			elif (out_filename == "lineorder-date-5-5.tbl"):
				date_5_5(batch_num, row, num_batches, debris)

			elif (out_filename == "lineorder-date-10-10.tbl"):
				date_10_10(batch_num, row, num_batches, debris)

			elif (out_filename == "lineorder-date-20-20.tbl"):
				date_20_20(batch_num, row, num_batches, debris)

			elif (out_filename == "lineorder-date-50-50.tbl"):
				date_50_50(batch_num, row, num_batches, debris)

			elif (out_filename == "lineorder-date-10-50.tbl"):
				date_10_50(batch_num, row, num_batches, debris)

			elif (out_filename == "lineorder-date-50-10.tbl"):
				date_50_10(batch_num, row, num_batches, debris)

			elif (out_filename == "lineorder-date-25-25.tbl"):
				date_25_25(batch_num, row, num_batches, debris)

			elif (out_filename == "lineorder-date-linear.tbl"):
				date_linear(batch_num, row, num_batches, debris)
				# debris[2] = str(random.randint(1,30000))
				# debris[3] = str(random.randint(1,200000))
				# debris[4] = str(random.randint(1,2000))


			elif (out_filename == "lineorder-date-linear-part-20-20.tbl"):
				date_linear(batch_num, row, num_batches, debris)
				part_20_20(batch_num, row, num_batches, debris)
				# debris[2] = str(random.randint(1,30000))
				# debris[4] = str(random.randint(1,2000))

			# elif (out_filename =="lineorder-date-linear-part-20-20-cust-10-10.tbl"):
			# 	date_linear(batch_num, row, num_batches, debris)
			# 	part_20_20(batch_num, row, num_batches, debris)
			# 	cust_10_10(batch_num, row, num_batches, debris)
			#	debris[4] = str(random.randint(1,2000))


			elif (out_filename == "lineorder-date-linear-part-20-20-supp-10-20.tbl"):
				date_linear(batch_num, row, num_batches, debris)
				part_20_20(batch_num, row, num_batches, debris)
				supp_10_20(batch_num, row, num_batches, debris)
			else:
				print("invalid filename")
				break

			line = "|".join(debris)
			print(line, file=outfile)


		batch_size = batch_size_file.readline()[:-1]
		batch_num += 1

	infile.close()
	outfile.close()


"""
Update a distribution dist such that the values at the inputted indices
increase linearly (while values at all other indices decrease linearly).

It is assumed that the distribution will be updated after every batch.
"""


def main():

	# GenerateRows("lineorder-date-5-5.tbl","batch-sizes-date.txt")
	# GenerateRows("lineorder-date-10-10.tbl","batch-sizes-date.txt")
	# GenerateRows("lineorder-date-20-20.tbl","batch-sizes-date.txt")
	# GenerateRows("lineorder-date-50-50.tbl","batch-sizes-date.txt")
	# GenerateRows("lineorder-date-10-50.tbl","batch-sizes-date.txt")
	# GenerateRows("lineorder-date-50-10.tbl","batch-sizes-date.txt")
	# GenerateRows("lineorder-date-25-25.tbl","batch-sizes-date.txt")

	# GenerateRows("lineorder-date-part-adversary.tbl","batch-sizes-adversary.txt")
	# GenerateRows("lineorder-date-linear.tbl","batch-sizes-date.txt")
	# GenerateRows("lineorder-date-first-half.tbl","batch-sizes-date.txt")

	# GenerateRows("lineorder-date-1-1.tbl","batch-sizes-date.txt")
	# GenerateRows("lineorder-date-2-1.tbl","batch-sizes-date.txt")
	# GenerateRows("lineorder-date-1-2.tbl","batch-sizes-date.txt")
	# GenerateRows("lineorder-date-2-2.tbl","batch-sizes-date.txt")

	GenerateRows("lineorder-date-linear-part-20-20.tbl","batch-sizes-date.txt")
	GenerateRows("lineorder-date-linear-part-20-20-supp-10-20.tbl","batch-sizes-date.txt")

if __name__ == "__main__":
	main()