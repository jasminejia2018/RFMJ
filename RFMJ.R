#  load Mobile data----
library(arules)
library(plyr)
library(dplyr)
library(ggplot2)
library(zoo)
library(grid)
library(scales)
startDate_history <- as.Date("20151209","%Y%m%d")
endDate_history <-Sys.Date()

##library and sql query------
#######LOAD DRIVERS WITH CREDENTIALS#######
options(java.parameters = "-Xmx32096m")
library(RJDBC)
library(RODBC)
library(lubridate)
library(data.table)
library(stringr)
library(dplyr)
library(highcharter)
library(reshape)

drv <- JDBC(driverClass="org.netezza.Driver", classPath ="/DataDrive/nzjdbc3.jar","'")
conn <- dbConnect(drv, "jdbc:netezza://cgunx60.na.compassgroup.corp:5480/CDL_STAGE", 'Keshev_Kulkarni', 'qat3gasT')
conn <- dbConnect(drv, "jdbc:netezza://172.29.13.60:5480/CDL_US_SOURCE", 'Keshev_Kulkarni', 'qat3gasT')



cn = odbcConnect("DashboardData", uid="microstrategy", pwd="s0ofKgLik6yhWjezy5L8")

mobile=dbGetQuery(conn,"SELECT ORDERID
                  , ORDERITEMID
                  , ITEMID
                  , MENUITEMNAME
                  , ITEMSIZE
                  , CUSTOMERID
                  , ORDERDATE
                  , ORDERDATETIME
                  , APPNAME
                  , UNITID
                  , UNIT_NAME
                  , UNIT_DETAIL_NAME
                  ,GROUP_NAME
                  ,GROUP_ID
                  , SECTOR
                  , STOREID
                  , STORENAME
                  , ADDRESS_COUNTRY
                  , TRANSACTIONTYPEREFID
                  , BRANDNAME
                  , PAYMENTTYPE
                  , VOLANTETRANSACTIONID
                  , PROMOTIONCODE
                  , DISCOUNTAMOUNT
                  , DISCOUNT_VALUEISAMOUNT
                  , QUANTITY
                  , SALES
                  , PRICECURRENCYCODE
                  , LIVE
                  , TIMEZONE
                  , LOAD_DATE
                  
                  FROM CDL_DATAMART..DH_DATAMART")

getDataFrame <- function(df,startDate,endDate){
  colnames(df)[which(names(df) == "ORDERDATE")] <- "dateOrder"
  df$dateOrder<-as.Date(ymd_hms(df$dateOrder))
  
  
  df <- df[order(df$dateOrder,decreasing = TRUE),]
  #df <- df[df$dateOrder>= startDate_history,]
  df <- df[df$dateOrder<= endDate_history,]
  mydata<-df
  mydata$recency=round(as.numeric(difftime(endDate_history,mydata$dateOrder,units="days")) )
  
  # Find the total spend of each customer
  mydataM=aggregate(mydata$SALES,list(mydata$CUSTOMERID),sum)
  # change names
  names(mydataM)=c("CUSTOMERID","Monetization")
  
  
  # Find frequency of trasaction for each customer
  mydataF=aggregate(mydata$ORDERID,list(mydata$CUSTOMERID),length)
  names(mydataF)=c("CUSTOMERID","Frequency")
  
  #Define recency for each customer
  mydataR=aggregate(mydata$recency,list(mydata$CUSTOMERID),min)
  names(mydataR)=c("CUSTOMERID","Recency")
  mydataR1=aggregate(mydata$recency,list(mydata$CUSTOMERID),max)
  names(mydataR1)=c("CUSTOMERID","Recency2")
  test2=merge(mydataR1,mydataR,"CUSTOMERID")
  test2$journey=test2$Recency2
  test2=test2[,c(1,4)]
  
  
  
  #Build RFM table
  test1=merge(mydataF,mydataR,"CUSTOMERID")
  salesRFM=merge(mydataM,test1,"CUSTOMERID")
  salesRFM=merge(salesRFM,test2,"CUSTOMERID")
  return(salesRFM)
}
customerSegmentation <- function(salesRFM){
  
  
  # Using the above exploratoy plots, we define the following user segements for Rogers Eats mobile app:
  #			Loyal Users: >20 orders, 0-6 days, 7-13 days
  # 		Repeat Users: 3-10 orders, 11-20 orders, 0-6 days, 7-13 days
  #			New Users: 1 order, 2 orders, 0-6 days, 7-13 days
  # 		One-Time Buyers: 1 order, 14-19 days, 20-29 days, >30 days
  # 		In-Active Users: 2 orders, 3-10 orders, 11-20 orders, >20 orders, 14-19 days, 20-29 days, >30 days
  

  newdata <-salesRFM
  newdata$moneyperday=newdata$Monetization/newdata$journey
  newdata$orderperday=newdata$Frequency/newdata$journey
  
  newdata$scoreR <- ifelse(newdata$Recency <7, 5,
                           ifelse(newdata$Recency <14, 4,
                                  ifelse(newdata$Recency <21, 3,
                                         ifelse(newdata$Recency <35, 2,
                                  1))))
  newdata$scoreF <- ifelse(newdata$orderperday < quantile(newdata$orderperday, 0.2), 1,
                           ifelse(newdata$orderperday < quantile(newdata$orderperday, 0.4), 2,
                                  ifelse(newdata$orderperday < quantile(newdata$orderperday, 0.6), 3,
                                         ifelse(newdata$orderperday < quantile(newdata$orderperday, 0.8), 4,
                                  5))))
  
  newdata$scoreM <- ifelse(newdata$moneyperday < quantile(newdata$moneyperday, 0.2), 1,
                                            ifelse(newdata$moneyperday < quantile(newdata$moneyperday, 0.4), 2,
                                                   ifelse(newdata$moneyperday< quantile(newdata$moneyperday, 0.6), 3,
                                                          ifelse(newdata$moneyperday < quantile(newdata$moneyperday, 0.8), 4,
                                                                 5))))
  newdata$scoreJ=ifelse(newdata$journey < quantile(newdata$journey, 0.5), 1,2)
  #newdata$totalscore=newdata$scoreR+newdata$scoreF+newdata$scoreM+newdata$scoreJ
  newdata$userSegment <- ifelse(newdata$journey<21, "New",
                                ifelse(newdata$scoreR %in% c(1,2,3)&newdata$scoreF%in% c(1,2), "Churned",
                                ifelse(newdata$scoreR %in% c(1,2)&newdata$scoreF%in% c(3,4,5),  'Churned Loyal',
                                ifelse(newdata$scoreR %in% c(3)&newdata$scoreF%in% c(3,4,5),  'Risk',
                                       ifelse(newdata$scoreR %in% c(4,5)&newdata$scoreF%in% c(1,2),  'Risk',

                                ifelse(newdata$scoreR %in% c(4,5)&newdata$scoreF%in% c(3,4,5), 'Loyal',

                                "n/a"))))))
  mydata=na.omit(mydata)
  mydata=full_join(mydata,newdata)
  return(mydata)
}

#Slide 2 Mobile App Users at Rogers ------
mydata<-getDataFrame(mobile,startDate_history,endDate_history)
customer_profile<-customerSegmentation(mydata)

