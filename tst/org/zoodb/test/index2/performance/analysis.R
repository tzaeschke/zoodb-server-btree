require(ggplot2)
theme_set(theme_gray(base_size = 24))
ggopts <- theme(axis.text.x=element_text(colour="#555555"),axis.text.y=element_text(colour="#555555")) + 
  theme(legend.text=element_text(size=22), legend.title=element_text(size=22)) 
  
image_directory <- "./"
fullData <- read.table('performanceTest.csv', sep=",",header=TRUE,quote="")
fullData <- fullData[fullData$ExperimentNumber > 0,]
fullData$Index <- fullData$IndexType
fullData$numElements = fullData$numElements/1000


# Unique Random Insert NumNodes
data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$ListType == "random"
                 & fullData$Operation == "insert"
                 ,] 
pdf(file=paste0(image_directory,"unique_random_insert_numNodes.pdf"))
ggplot(data, aes(x=numElements, y=NumNodes, col=Index)) + 
  geom_smooth(method=lm, size=2.5) + 
  geom_point(size=5) + 
  scale_colour_manual(values=c("#BF3c04", "#BF9104")) +
  xlab("Entries (thousand)") +  
  ylab("Pages") + 
  ggopts
dev.off()

# Unique Increasing Insert NumNodes
data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$Operation == "insert" 
                 & fullData$ListType == "increasing"
                 ,]

pdf(file=paste0(image_directory,"unique_increasing_insert_numNodes.pdf"))
ggplot(data, aes(x=numElements, y=NumNodes, col=Index)) + 
  geom_smooth(method=lm, size=2.5) + 
  geom_point(size=5) + 
  scale_colour_manual(values=c("#BF3c04", "#BF9104")) + 
  xlab("Entries (thousand)") +  
  ylab("Pages") + 
  ggopts
dev.off()

# NonUnique Random Insert NumNodes
data <- fullData[fullData$IndexUnique == "nonUnique" 
                 & fullData$ListType == "random_nonUnique"
                 & fullData$Operation == "insert"
                 ,] 
pdf(file=paste0(image_directory,"nonunique_random_insert_numNodes.pdf"))
ggplot(data, aes(x=numElements, y=NumNodes, col=Index)) + 
  geom_smooth(method=lm, size=2.5) + 
  geom_point(size=5) + 
  scale_colour_manual(values=c("#BF3c04", "#BF9104")) + 
  xlab("Entries (thousand)") +  
  ylab("Pages") + 
  ggopts
dev.off()

# Unique Increasing Insert
data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$Operation == "insert" 
                 & fullData$ListType == "increasing"
                 ,]

pdf(file=paste0(image_directory,"unique_increasing_insert.pdf"))
ggplot(data, aes(x=numElements, y=Duration, col=Index)) + 
  geom_smooth(method=lm, size=2.5, se=FALSE) + 
  scale_colour_manual(values=c("#BF3c04", "#BF9104")) + 
  xlab("Entries (thousand)") +  
  ylab("Duration in ms") + 
  geom_point(size=5) + 
  ggopts
dev.off()

# NonUnique Random Insert
data <- fullData[fullData$IndexUnique == "nonUnique" 
                 & fullData$ListType == "random_nonUnique"
                 & fullData$Operation == "insert"
                 ,] 
pdf(file=paste0(image_directory,"nonunique_random_insert.pdf"))
ggplot(data, aes(x=numElements, y=Duration, col=Index)) + 
  geom_smooth(method=lm, size=2.5, se=FALSE) + 
  scale_colour_manual(values=c("#BF3c04", "#BF9104")) + 
  xlab("Entries (thousand)") +  
  ylab("Duration in ms") + 
  geom_point(size=4) + 
  ggopts
dev.off()


# Unique Increasing Insert Write
data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$Operation == "insert_write" 
                 & fullData$ListType == "increasing"
                 ,]
pdf(file=paste0(image_directory,"unique_increasing_write.pdf"))
ggplot(data, aes(x=numElements, y=Duration, col=Index)) + 
  geom_smooth(method=lm, size=2.5, se=FALSE) + 
  scale_colour_manual(values=c("#BF3c04", "#BF9104")) + 
  xlab("Entries (thousand)") +  
  ylab("Duration in ms") + 
  geom_point(size=4) + 
  ggopts
dev.off()

# Unique Increasing Search
data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$Operation == "search" 
                 & fullData$ListType == "increasing" 
                 ,]
pdf(file=paste0(image_directory,"unique_increasing_search.pdf"))
ggplot(data, aes(x=numElements, y=Duration, col=Index)) + 
  geom_smooth(method=lm, size=2.5, se=FALSE) + 
  geom_point(size=4) + 
  scale_colour_manual(values=c("#BF3c04", "#BF9104")) + 
  xlab("Entries (thousand)") +  
  ylab("Duration in ms") + 
  ggopts
dev.off()

# Unique Increasing Remove
data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$Operation == "remove" 
                 & fullData$ListType == "random"
                 ,]
pdf(file=paste0(image_directory,"unique_increasing_remove.pdf"))
ggplot(data, aes(x=numElements, y=Duration, col=Index)) + 
  geom_smooth(method=lm, size=2.5, se=FALSE) + 
  geom_point(size=4) + 
  scale_colour_manual(values=c("#BF3c04", "#BF9104")) + 
  xlab("Entries (thousand)") +  
  ylab("Duration in ms") + 
  ggopts
dev.off()

# Unique Random Remove NumNodes
data <- fullData[fullData$IndexUnique == "Unique" 
                 & fullData$Operation == "remove" 
                 & fullData$ListType == "random"
                 ,]
pdf(file=paste0(image_directory,"unique_random_remove_numNodes.pdf"))
ggplot(data, aes(x=numElements, y=NumNodes, col=Index)) + 
  geom_smooth(method=lm, size=2.5, se=FALSE) + 
  geom_point(size=4) + 
  scale_colour_manual(values=c("#BF3c04", "#BF9104")) + 
  xlab("Entries (thousand)") +  
  ylab("Number of nodes") + 
  ggopts
dev.off()

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
fullData$IndexRole <- fullData$Index
fullData$Index <- fullData$IndexType

pdf(file=paste0(image_directory,"SO_sizes.pdf"))
ggplot(data=fullData, aes(x=IndexRole, y=Pages, fill=Index)) + geom_bar(stat="identity", position=position_dodge()) + 
  scale_fill_manual(values=c("#BF3c04", "#BF9104")) + ggopts +xlab("Index role")
dev.off()

#########################
# StackExchange Posts
########################
fullData <- read.table('StackExchange_posts.csv', sep=",",header=TRUE,quote="")
n <- nrow(fullData)
fullData <- rbind(fullData,fullData)
fullData["Duration"] <- NA
fullData["Index"] <- NA
fullData[1:n,]$Duration <- fullData[1:n,]$New
fullData[1:n,]$Index <- "new"
fullData[(n+1):(2*n),]$Duration <- fullData[1:n,]$Old
fullData[(n+1):(2*n),]$Index <- "old"
fullData$Index <- as.factor(fullData$Index)
fullData$Posts <- fullData$Posts/1000000
data<-fullData
data$Duration <- data$Duration/1000

pdf(file=paste0(image_directory,"SO_commit_duration.pdf"))
ggplot(data=data, aes(x=Posts, y=Duration, group=Index, colour=Index)) + 
  geom_point(size=4) +
  ggopts + ylim(c(0,37)) + scale_colour_manual(values=c("#BF3c04", "#BF9104")) + xlab("Posts (million)") + ylab("Duration in s")
dev.off()