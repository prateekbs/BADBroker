import re

f1 = open('EmergencyReports.json', 'r')
f2 = open('EmergencyReports.adm', 'w')
for line in f1:
	splitLine = line.split()

	##First replce the circle
	radius = float(splitLine[4].split(",")[0])
	x = float(splitLine[9].split("}")[0])
	y = float(splitLine[7].split(",")[0])
	newCircle = "circle(\"" + str(x) + "," + str(y) + " " + str(radius) + "\")"
	print newCircle
	start = line.find("{\"r")
	end = line.find("}}")
	line = line[:start] + newCircle + line[end+2:]
	#print line
	f2.write(line)
    

    ##Now replace the timestamp

f1.close()
f2.close()