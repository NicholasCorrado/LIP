
import random

input_dir = "./benchmarks/benchmark-original/"
output_dir = "./benchmarks/benchmark-skew/"


class BinomialDistribution:

	groundSet = []
	p = 0

	def __init__(self, groundSet, p):
		self.groundSet = groundSet
		self.p = p

	def sample(self):
		s = 0
		n = len(self.groundSet)
		for i in range(n):
			if random.random() < self.p:
				s += 1
		return self.groundSet[s]


class UniformDistribution:
	groundSet = []

	def __init__(self, groundSet):
		self.groundSet = groundSet

	def sample(self):
		index = int(len(self.groundSet) * random.random())
		return self.groundSet[index]


class RandomDistribution:

	groundSet = []
	seeds = []
	dist = []
	def __init__(self, groundSet):

		self.groundSet = groundSet
		n = len(groundSet)

		seeds = [random.random() for i in range(n)]

		s = sum(seeds)

		self.seeds = [seed/s for seed in seeds]
		self.dist = [0 for i in range(n+1)]
		self.dist[0] = 0
		for i in range(1, n+1):
			self.dist[i] = self.dist[i-1] + self.seeds[i-1]

	def sample(self):
		coin = random.random()

		lo = 0
		hi = len(self.groundSet)

		while lo <= hi:
			mid = (lo + hi) // 2

			if self.dist[mid] <= coin and coin < self.dist[mid+1]:
				return self.groundSet[mid]

			elif self.dist[mid] > coin:
				hi = mid - 1
			elif self.dist[mid+1] <= coin:
				lo = mid + 1

		return self.groundSet[0]


class SquareWaveDistribution:

	groundSet = []
	period = 0
	ratio = 0

	def __init__(self, groundSet, period, ratio):
		self.groundSet = groundSet
		self.period = period
		self.ratio = ratio

	def sample(self):
		index = int(len(self.groundSet) * random.random())

		return self.groundSet[index]



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
	partDist = RandomDistribution(partKeyList)
	suppDist = RandomDistribution(suppKeyList)
	dateDist = RandomDistribution(dateKeyList)

	print("# Custkey:", len(custKeyList))
	print("# Partkey:", len(partKeyList))
	print("# Suppkey:", len(suppKeyList))
	print("# Datekey:", len(dateKeyList))

	for i in range(rows):
		if (i % (rows / 100) == 0):
			print(i / (rows / 100), "printed.")
		toPrint = sampleLine

		toPrint[FIELD["CUST KEY"]] = custDist.sample()
		toPrint[FIELD["PART KEY"]] = partDist.sample()
		toPrint[FIELD["SUPP KEY"]] = suppDist.sample()
		toPrint[FIELD["ORDER DATE"]] = dateDist.sample()

		toPrintLine = "|".join(toPrint)
		print(toPrintLine, file=outfile)
	outfile.close()


def TestRandomDist():
	gs = [i for i in range(10)]

	D = RandomDistribution(gs)

	count = {}
	num = 10000
	for i in range(num):
		d = D.sample()
		if d not in count:
			count[d] = 1
		else:
			count[d] += 1
	for key in count:
		count[key] = count[key] / num
		print(key, abs(count[key] - D.seeds[key]))


def main():
	GenerateRows(10000000)

	


if __name__ == "__main__":
	main()