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

GROUPBYFBICODE = group USACRIMEDATA by FBICode;

COUNTCASEBYFBICODE = foreach GROUPBYFBICODE generate group as fbicode,COUNT(USACRIMEDATA) as count;

store COUNTCASEBYFBICODE into '/user/acadgild/project/1.1_countcasebyfbicode';
