
input_dir = "./benchmarks/benchmark-original/"
output_dir = "./benchmarks/benchmark-skew/"


def skew(tablename, condition, time):
	table_file = input_dir + tablename
	output_file = output_dir + tablename

	infile = open(table_file, "r")
	outfile = open(output_file, "w")
	
	line = infile.readline()[:-1]
	while line != "":
		if condition(line):
			for i in range(time)
				print(line, file=outfile)
		line = infile.readline()[:-1]

	infile.close()
	outfile.close()

def main():
	tablename = "date.tbl"

	def date_condition(line):
		return line.split("|")[4] == "1992"
	def supplier_condition(line):
		return line.split("|")
	def dummy(line):
		return True

	skew("date.tbl", date_condition, )
	skew("part.tbl", dummy, 1)
	skew("supplier.tbl", dummy, 1)
	skew("customer.tbl", dummy, 1)
	skew("lineorder.tbl", dummy, 1)
	
	

main()