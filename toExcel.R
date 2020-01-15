library(RODBC)
library(plyr)
library(climtrends)
library(xlsx)
library(miscTools)
library(psych)
library(stringr)

#### CHOOSE FUNCTION (type in letter function code)
funct = "HC"


###where do you want your files to save to
setwd("./validation/data/HC/2018")


####open connection with SQL and pull data and close connection (set up RODBC connection prior)
odbcChannel <- odbcConnect("WHAT IS YOUR ODBC CONNECT NAME")
pma <- sqlFetch(odbcChannel, "PMA_METRIC_DATA")
#pma <- sqlFetch(odbcChannel, "PMA_CSS")
lookup <- sqlFetch(odbcChannel,"PMA_METRIC_CD_LOOKUP_ELEMENT")
close(odbcChannel)




#### Clean Look_up values
look <- lookup[grep("V", lookup$CALC_TYPE), ]
look <- look[grep("2018", look$DATAYEAR), ]
cols = c("METRIC_CODE","METRIC")
look <- look[cols]
names(look)[1] <- "METRIC_CODE"
names(look)[2] <- "METRIC_NAME"

####################### add numerators and denom####
pma$nums <- rowSums(cbind(pma$NUM1,pma$NUM2,pma$NUM3,pma$NUM4,pma$NUM5,pma$NUM6 ,pma$NUM7,pma$NUM8 ,pma$NUM9),na.rm=TRUE) 
pma$dens <- rowSums(cbind(pma$DEN1,pma$DEN2,pma$DEN3 ,pma$DEN4),na.rm=TRUE) 

##### subset into only these columns################
pa <- c("dataYear","AGENCY_SHORT","BUREAU","VALUE","METRIC_CODE","nums","dens")
pma$AGENCY_SHORT <- toupper(pma$AGENCY_SHORT)
p <-pma[pa]

###################################
### break into years ##############

year <- p[ which(p$dataYear==2017),]
w <- reshape(year,timevar = "dataYear",idvar = c("dataYear", "AGENCY_SHORT"  ,     "BUREAU", "METRIC_CODE"),direction = "wide")
w14<-w[ , ! apply( w , 2 , function(x) all(is.na(x)) ) ]
year <- p[ which(p$dataYear==2018),]
w <- reshape(year,timevar = "dataYear",idvar = c("dataYear", "AGENCY_SHORT"  ,     "BUREAU", "METRIC_CODE"),direction = "wide")
w15<-w[ , ! apply( w , 2 , function(x) all(is.na(x)) ) ]

#######################################################################
############# outlier stuff
new <- w15[grep(funct, w15$METRIC_CODE), ]
new <- new[c(1:4)]
new <- reshape(new,timevar = "METRIC_CODE",idvar = c ("AGENCY_SHORT"  ,     "BUREAU"),direction = "wide")

a= ncol(new)
index <-new[1:2]
for(i in 3:a)
{
  nums <- FindOutliersQuant(new[i])
  outs <-as.data.frame(new[nums,c(1,2, i)])
  b = nrow(outs)
  if(b >0 ){
    outs[3] <- "Outlier"
  }
  ins <- as.data.frame(new[-nums,c(1,2,i)])
  c=nrow(ins)
  if(c >0 ){
    ins[3] <- ""
  }
  outliers <-rbind(outs,ins)
  index <- merge(outliers,index,c(1:2),all=TRUE)
  i=i+1
}
###################################################


##calcuate Government-wide Average
averages <- new[grep("Agency Wide",new$BUREAU),]
avs <- data.frame(colMedians(averages[3:ncol(averages)],na.rm = TRUE))


### creat our main dataframe called all this subsets data by the funct choosen
all <- merge(w14,w15,c(1:3),all=TRUE)
all <- merge(all,look,by="METRIC_CODE",all.x=TRUE)
all <- all[grep(funct, all$METRIC_CODE), ]


### gets a list of uniques metrics and agencies for the loop below
agencies <- unique(all$AGENCY_SHORT)
all2 <- all[complete.cases(all$VALUE.2017),]
metrics <- unique(all2$METRIC_CODE)
agencies <- c("DOJ")
## Don't use these
#all$METRIC_CODE <- substr(all$METRIC_CODE, 1, 2)
metrics = c("HC16","HC02","HC03","HC04","HC31","HC76","HC75",'HC35','HC39',"HC37","HC29","HC47","HC48","HC50",'HC51','HC52','HC41','HC113')
#metrics = c("IT02")
#metrics = c("HC16","HC51")
#metrics = c("ACQ06")
#metrics = c("FM02","FM11","FM13","FM21","FM22","FM23","FM24","FM25","FM26","FM27","FM28","FM29","FM30")
#metrics =c("ACQ01","ACQ02","ACQ03","ACQ04","ACQ06","ACQ07","ACQ17","ACQ22","ACQ23")
################# ###################################### Write to EXCEL ###############################

for (a in agencies){
 temp <- all[grep(a, all$AGENCY_SHORT), ]
 for (m in metrics){
  #temp2 <- temp[grep(m, temp$METRIC_CODE), ]
  temp2 <- temp[ which(temp$METRIC_CODE==m),]
  number = grep(m, colnames(index))
  sub <- index[c(1,2,number)]
  ada <- avs[grep(m,row.names(avs)),]
  try(temp2 <- merge(temp2,sub,by=c("AGENCY_SHORT","BUREAU")))
  try(temp2$Government_Wide_Median <- ada)
  try(temp2$Percent_Change_Previous_Year <- (temp2$VALUE.2016-temp2$VALUE.2015)/temp2$VALUE.2015)
  try(s <- unique(temp2$METRIC_NAME))
  try(temp2 <- temp2[c(1,2,4:ncol(temp2))])
  try(names(temp2)[1] <- "Agency")
  try(names(temp2)[2] <- "Bureau")
  try(names(temp2)[3] <- "Value_2017")
  try(names(temp2)[4] <- "Numerators_2017")
  try(names(temp2)[5] <- "Denominators_2017")
  try(names(temp2)[6] <- "Value_2018")
  try(names(temp2)[7] <- "Numerators_2018")
  try(names(temp2)[8] <- "Denominators_2018")
  try(names(temp2)[9] <- "Metric_Name")
  try(names(temp2)[10] <- "Outlier_Status_2018_Value")
  #try(temp2 <- sapply(temp2, as.character)) # since your values are `factor`
  
  #try( temp2[is.na(temp2)] <- "NULL")
  try(temp2 <- data.frame(temp2))
  l= ncol(temp2)
  try(ifelse(l==1,temp2 <- data.frame(t(temp2)),temp2 <- temp2))
  try(temp3 <- temp2[c(1,2,9,3,6,10,11,4,7,5,8)])
  try(temp3 <- temp2[c(1,2,9,3,6,10,11,12,4,7,5,8)])
  try( s <- gsub("[^a-zA-Z0-9]","",s)  )

  try(write.xlsx(temp3, file=paste(a,"_",funct,".xlsx"),sheetName=paste(m,"_",s),append = TRUE, row.names = FALSE,showNA = FALSE))
 }
}


