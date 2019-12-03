import random


input_dir = "./benchmarks/benchmark-1/"
output_dir = "./benchmarks/benchmark-skew/"



def date_linear(batch_num, i, line):
	
	debris = line.split("|")

	if (debris[5].startswith("1997") or debris[5].startswith("1998")):
		debris[5] = "19920101"
	if random.random() < batch_num/NUM_BATCHES:
		debris[5] = "19970101"

	return "|".join(debris)


def adversary(batch_num, i, line):
	
	debris = line.split("|")

	if batch_num == 0: 
		# date selectivity is 2/5 for the first batch
		if i < 6000000//NUM_BATCHES*0.4:
			debris[5] = "19970101"
			# datepass += 1
		else:
			debris[5] = "19920101"
		# 3/5 of the tuples that pass the date selection should pass the part selection,
		# but the overall part selectivity for the batch should be less than the date selectivity.
		if i < 6000000//NUM_BATCHES*0.4*0.6:
			debris[3] = "1"
			# partpass += 1
		else:
			debris[3] = "-1"


	# date selectivity = 1 on even batches
	# part selectivity = 0 on even batches
	else:
		if batch_num % 2 == 1:
			debris[5] = "19970101"
			debris[3] = "-1"
		# date selectivity = 0 on odd batches (after first batch)
		else:
			debris[5] = "19920101"
			debris[3] = "1"

	debris[2] = "8450"
	debris[4] = "761"

	return "|".join(debris)



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






	
			# if (debris[5].startswith("1997") or debris[5].startswith("1998")):
			# 	debris[5] = "19920101"

			# if random.random() < batch_num/NUM_BATCHES:
			# 	debris[5] = "19970101"

			# odd batches have no 1993 keys
			# even batcheas have only 1993 keys
			# if batch_num % 20 < 10:
			# 	debris[5] = "19970101"
			# 	print("1997")
			# elif (debris[5].startswith("1997") or debris[5].startswith("1998")):
			# 	debris[5] = "19920101"
			# 	print("else")

			# if batch_num < NUM_BATCHES//2:
			# 	debris[5] = "19970101"
			# elif (debris[5].startswith("1997") or debris[5].startswith("1998")):
			# 	debris[5] = "19920101"
			
			# debris[3] = "-1"
			# if batch_num % 4 < 2:
			# 	debris[3] = "1"

			#debris[2] = str(random.randint(1,300000))
			#debris[4] = str(random.randint(1,2000))