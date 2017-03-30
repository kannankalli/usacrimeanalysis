USACRIMEDATA = load '/user/acadgild/project/USACRIMEANALYSIS/Crimes_2001_to_present.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER')
AS 
(ID:long,
CaseNumber:chararray,
Date:chararray,
Block:chararray,
IUCR:int,
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

FILTERBYFBICODE32 = filter USACRIMEDATA by FBICode == '32';
GROUPBYFBICODE32 = group FILTERBYFBICODE32 by FBICode;

COUNTCASEBYFBICODE32 = foreach GROUPBYFBICODE32 generate COUNT(FILTERBYFBICODE32) as count;

store COUNTCASEBYFBICODE32 into '/user/acadgild/project/1.2_countcasebyfbicode32';
