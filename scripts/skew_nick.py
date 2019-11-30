
input_dir = "../benchmarks/benchmark-1/"
output_dir = "../benchmarks/benchmark-skew/"


# Make duplicates of tuples satisfying some condition.
# The original tuples are written to the file in addition to their duplicate(s).
# Duplicates are assigned  unique (arbitrary) key.
def skew(tablename, condition, time):
	table_file = input_dir + tablename
	output_file = output_dir + tablename

	infile = open(table_file, "r")
	outfile = open(output_file, "w")
	
	key = 424242 # some arbtrary key that probably doesn't already exist. We will increment this as we write more duplicates.

	line = infile.readline()[:-1]
	while line != "":
		print(line, file=outfile)
		if condition(line):
			for i in range(time):
				# Don't just duplicate the line; make sure you assign it a unique key too! 
				# Otherwise the Bloom filter isn't accurate!
				new_list = line.split("|")
				new_line = "|".join(new_list)
				print(new_line, file=outfile)	
		line = infile.readline()[:-1]

	infile.close()
	outfile.close()

def main():
	tablename = "date.tbl"

	def date_condition(line):
		return line.split("|")[4] == "1993"
	def supplier_condition(line):
		return line.split("|")
	def dummy(line):
		return True
	def customer_condition(line):
		return line.split("|")[5] == "AMERICA"
	def part_condition(line):
		return True

	skew("date.tbl", date_condition, 1)
	#skew("part.tbl", dummy, 1)
	#skew("supplier.tbl", dummy, 1)
	skew("customer.tbl", customer_condition, 2)
	#skew("lineorder.tbl", dummy, 1)
	
	

main()