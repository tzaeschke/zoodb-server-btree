require(ggplot2)
theme_set(theme_gray(base_size = 24))
ggopts <- theme(axis.text.x=element_text(colour="#555555"),axis.text.y=element_text(colour="#555555")) + theme(legend.text=element_text(size=22), legend.title=element_text(size=22))
image_directory <- "~/gitProjects/islab-2014-presentation/presentation/images/"
fullData <- read.table('performanceTest.csv', sep=",",header=TRUE,quote="")
fullData <- fullData[fullData$ExperimentNumber > 0,]

#####################
# Unique Random Insert
#####################

data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$ListType == "random"
                 & fullData$Operation == "insert"
                 & fullData$numElements == 1000000
                 ,] 

qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique key indices",
      xlab="", ylab="Duration in ms") + 
      #scale_y_continuous(limits = c(0, 1500)) + 
      ylim(0,1800) +
      expand_limits(y=0) + 
      scale_x_discrete(breaks=c("new","old"), labels=c("new Index", "old Index"))

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
pdf(file=paste0(image_directory,"unique_random_insert.pdf"))
qplot(data$numElements, data$Duration, data=data, geom=c("point", "smooth"),
      method="lm", formula=y~x, color=IndexType,
      main="Insert in unique key indices",
      xlab="Number of Elements", ylab="Duration") + ggopts
dev.off()

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
                 & fullData$numElements == "1000000"
                 ,] 
qplot(data$IndexType, data$Duration, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique key-value indices",
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

pdf(file=paste0(image_directory,"nonUnique_random_insert_nodes.pdf"))
qplot(data$numElements, data$NumNodes, data=data, geom=c("point", "smooth"),
      method="lm", formula=y~x, color=IndexType,
      main="Insert in unique key-value indices",
      xlab="Number of elements", ylab="Number of pages") + ggopts
dev.off()

data <- fullData[fullData$IndexUnique == "nonUnique" 
                 & fullData$ListType == "random_nonUnique"
                 & fullData$Operation == "insert_write"
                 ,]
pdf(file=paste0(image_directory,"nonUnique_random_write.pdf"))
qplot(data$numElements, data$Duration, data=data, geom=c("point", "smooth"), 
      method="lm", formula=y~x, color=IndexType,
      main="Write unique key-value indices",
      xlab="Number of elements", ylab="Duration") + ggopts
dev.off()

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

qplot(data$IndexType, data$NumNodes, data=data, geom=c("boxplot", "jitter"),
      fill=IndexType, main="Performance of Unique indices",
      xlab="", ylab="number of nodes") 

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


########################
# StackExchange Index size
#######################
fullData <- read.table('StackExchange_sizes.csv', sep=",",header=TRUE,quote="")

pdf(file=paste0(image_directory,"SO_sizes.pdf"))
ggplot(data=fullData, aes(x=Index, y=Pages, fill=IndexType)) + geom_bar(stat="identity", position=position_dodge()) + 
  scale_fill_manual(values=c("#BF3c04", "#BF9104")) + ggopts
dev.off()

#########################
# StackExchange Posts
########################
fullData <- read.table('StackExchange_posts.csv', sep=",",header=TRUE,quote="")
n <- nrow(fullData)
fullData <- rbind(fullData,fullData)
fullData["Duration"] <- NA
fullData["IndexType"] <- NA
fullData[1:n,]$Duration <- fullData[1:n,]$New
fullData[1:n,]$IndexType <- "new"
fullData[(n+1):(2*n),]$Duration <- fullData[1:n,]$Old
fullData[(n+1):(2*n),]$IndexType <- "old"
fullData$IndexType <- as.factor(fullData$IndexType)
fullData$Posts <- fullData$Posts/1000000
data<-fullData
data$Duration <- data$Duration/1000

pdf(file=paste0(image_directory,"SO_commit_duration.pdf"))
ggplot(data=data, aes(x=Posts, y=Duration, group=IndexType, colour=IndexType)) + 
  #geom_line(size=1.5) + 
  geom_point(size=4) +
  # geom_smooth(method=lm, size=2) +
  ggopts + ylim(c(0,37)) + scale_colour_manual(values=c("#BF3c04", "#BF9104")) + xlab("Posts (million)") + ylab("Duration in s")
dev.off()

  
  # scale_shape_manual(values=c(22,21))
# qplot(data$Posts, data$Duration, data=data, geom=c("point", "smooth"),
#       method="lm", formula=y~x, color=IndexType,
#       xlab="Number of elements in millions", ylab="Duration in s") +
#       ggopts + ylim(c(0,37)) + scale_colour_manual(values=c("#BF3c04", "#BF9104"))
#  #       main="Commit duration for posts",