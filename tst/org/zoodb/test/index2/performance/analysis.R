require(ggplot2)
fullData <- read.table('performanceTest.csv', sep=",",header=TRUE,quote="")
fullData <- fullData[fullData$ExperimentNumber > 0,]

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
      xlab="", ylab="Duration in ms") + expand_limits(y=c(0,700)) + scale_y_continuous(expand = c(0, 0))

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

qplot(data$numElements, data$NumNodes, data=data, geom=c("point", "smooth"),
      method="lm", formula=y~x, color=IndexType,
      main="Performance of Unique indices",
      xlab="Number of Elements", ylab="Duration")

#####################
# NonUnique Random Insert
#####################
data <- fullData[fullData$IndexUnique == "nonUnique" 
                 & fullData$ListType == "random_nonUnique"
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


data <- fullData[fullData$IndexUnique == "nonUnique" 
                 & fullData$ListType == "random_nonUnique"
                 & fullData$Operation == "insert"
                 ,] 
qplot(data$numElements, data$Duration, data=data, geom=c("point", "smooth"),
      method="lm", formula=y~x, color=IndexType,
      main="Performance of Unique indices",
      xlab="Number of Elements", ylab="Duration")


qplot(data$numElements, data$NumNodes, data=data, geom=c("point", "smooth"),
      method="lm", formula=y~x, color=IndexType,
      main="Performance of Unique indices",
      xlab="Number of Elements", ylab="Number of nodes")

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


data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$Operation == "search" 
                 & fullData$ListType == "random"
                 ,]
qplot(data$numElements, data$Duration, data=data, geom=c("point", "smooth"),
      method="lm", formula=y~x, color=IndexType,
      main="Performance of Unique indices",
      xlab="Number of Elements", ylab="Duration")

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


#####################
# nonUnique Random Remove
#####################
data <- fullData[fullData$IndexUnique == "nonUnique" 
                 & fullData$Operation == "remove" 
                 & fullData$ListType == "random"
                 & fullData$numElements == 500000
                 ,]
qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="Duration in ms") 

#########################
# JDO Harness
########################
fullData <- read.table('manualPerformanceTest.csv', sep=",",header=TRUE,quote="")
data <- fullData[fullData$Operation ==  "jdo_test",]
qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance in JDO test harness",
      xlab="", ylab="Duration in ms") 


#########################
# Pole
########################
fullData <- read.table('manualPerformanceTest.csv', sep=",",header=TRUE,quote="")
data <- fullData[fullData$Operation ==  "pole",]
qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, range=0, main="Performance in PolePosition benchmark",
      xlab="", ylab="Duration in ms") 