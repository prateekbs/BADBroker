import re

f1 = open('UserLocations.json', 'r')
f2 = open('UserLocations.adm', 'w')
for line in f1:
	splitLine = line.split(":")

	x = float(splitLine[3].split("}")[0])
	y = float(splitLine[2].split(",")[0])
	newPoint = "point(\"" + str(x) + "," + str(y) + "\")"
	#print newCircle
	start = line.find("{\"y")
	end = line.find("}")
	line = line[:start] + newPoint + line[end+1:]
	#print line
	f2.write(line)
    


f1.close()
f2.close()

#point("30.0,70.0")