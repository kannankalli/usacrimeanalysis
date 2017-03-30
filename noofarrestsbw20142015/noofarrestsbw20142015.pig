REGISTER /usr/local/pig/lib/UDF.jar;
USACRIMEDATA = load '/home/acadgild/Downloads/Crimes_2001_to_present.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER')
AS 
(ID:long,
CaseNumber:chararray,
Date:chararray,
Block:chararray,
IUCR:chararray,
PrimaryType:chararray,
Description:chararray,
LocationDescription:chararray,
Arrest:chararray,
Domestic:chararray,
Beat:int,
District:int,
Ward:int,
CommunityArea:int,
FBICode:chararray,
XCoordinate:long,
YCoordinate:long,
Year:int,
UpdatedOn:chararray,
Latitude:double,
Longitude:double,
Location:chararray);



FILTERBYARRESTDATE = filter USACRIMEDATA by Arrest == 'true' AND ToDate(com.bigdata.acadgild.ConvertDateIntoFormat(Date),'MM/dd/yyyy') >= ToDate('20141001','yyyyMMdd') AND ToDate(com.bigdata.acadgild.ConvertDateIntoFormat(Date),'MM/dd/yyyy') <= ToDate('20151030','yyyyMMdd');

GROUPBY = group FILTERBYARRESTDATE by Arrest;

COUNTALLARREST = foreach GROUPBY generate COUNT(FILTERBYARRESTDATE) as count;

store COUNTALLARREST into '/home/acadgild/Downloads/1.4_arrest_10_2014_2015';

