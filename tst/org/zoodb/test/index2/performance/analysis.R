data <- read.table('performanceTest.csv', sep=",",header=T,quote="")

insertRandom <- data[data$Operation == "insert" & data$ListType == "random",]
insertIncreasing <- data[data$Operation == "insert" & data$ListType == "increasing",]
search <- data[data$Operation == "search",]
remove <- data[data$Operation == "remove",]

plot(insertRandom[insertRandom$numElements == 200000,]$Duration)
plot(density(insertRandom[insertRandom$numElements == 200000,]$Duration))

plot(insertRandom[insertRandom$numElements == 1000000,]$Duration)
plot(density(insertRandom[insertRandom$numElements == 1000000,]$Duration))
plot(insertRandom$numElements, insertRandom$Duration)

plot(insertIncreasing[insertIncreasing$numElements == 2000000,]$Duration)
plot(density(insertIncreasing[insertIncreasing$numElements == 2000000,]$Duration))
plot(insertIncreasing$numElements, insertIncreasing$Duration)

plot(search[search$numElements == 1000000,]$Duration)
plot(density(search[search$numElements == 1000000,]$Duration))
plot(search$numElements, search$Duration)

plot(remove[remove$numElements == 1000000,]$Duration)
plot(density(remove[remove$numElements == 1000000,]$Duration))
plot(remove$numElements, remove$Duration)