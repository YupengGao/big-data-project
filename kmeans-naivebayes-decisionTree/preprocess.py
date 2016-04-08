import re
inputpath = "/Users/pengpeng/Desktop/itemusermat"
outputpath = "/Users/pengpeng/Desktop/out.txt"
file_object = open(inputpath)
output = open(outputpath,'w')
for line in file_object:
	result = re.split(' ', line)
	list = ""
	for index in range(1, len(result)):
		if index == len(result) - 1:
			list += result[index]
		else:
			list += result[index] + " "
	output.write(list)
