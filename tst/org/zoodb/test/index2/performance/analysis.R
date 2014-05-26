require(ggplot2)
fullData <- read.table('performanceTest.csv', sep=",",header=TRUE,quote="")

#####################
# Unique Random Insert
#####################

data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$ListType == "random"
                 & fullData$Operation == "insert"
                 & fullData$numElements == 500000
                 ,] 
qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="Duration in ms") 

qplot(data$IndexType, data$NumNodes, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="number of nodes") 

qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="write duration") 

data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$ListType == "random"
                 & fullData$Operation == "insert"
                 ,] 
qplot(data$numElements, data$Duration, data=data, geom=c("point", "smooth"),
      method="lm", formula=y~x, color=IndexType,
      main="Performance of Unique indices",
      xlab="Number of Elements", ylab="Duration")

#####################
# NonUnique Random Insert
#####################
data <- fullData[fullData$IndexUnique == "nonUnique" 
                 & fullData$ListType == "random"
                 & fullData$Operation == "insert"
                 & fullData$numElements == "500000"
                 ,] 
qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="Duration in ms") 

qplot(data$IndexType, data$NumNodes, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="number of nodes") 

qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="write duration") 

#####################
# Unique Increasing Insert
#####################
data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$Operation == "insert" 
                 & fullData$ListType == "increasing"
                 & fullData$numElements == 1000000
                 ,]
qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="Duration in ms") 

qplot(data$IndexType, data$NumNodes, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="number of nodes") 

qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="write duration") 


#####################
# nonUnique Increasing Insert
#####################
data <- fullData[fullData$IndexUnique == "nonUnique" 
                 & fullData$Operation == "insert" 
                 & fullData$ListType == "increasing"
                 & fullData$numElements == 1000000
                 ,]
qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="Duration in ms") 

qplot(data$IndexType, data$NumNodes, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="number of nodes") 

qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="write duration") 


#####################
# Unique Random Search
#####################
data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$Operation == "search" 
                 & fullData$ListType == "random"
                 & fullData$numElements == 500000
                 ,]
qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="Duration in ms") 

#####################
# Unique Random Remove
#####################
data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$Operation == "remove" 
                 & fullData$ListType == "random"
                 & fullData$numElements == 500000
                 ,]
qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="Duration in ms") 


###############
# Misc
##############
plot(insertRandom[insertRandom$numElements == 200000,]$Duration)

insertIncreasing <- data[data$Operation == "insert" & data$ListType == "increasing",]
search <- data[data$Operation == "search",]
remove <- data[data$Operation == "remove",]

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
