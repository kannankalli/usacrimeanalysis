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

FILTERBYARREST = filter USACRIMEDATA by Arrest == 'true';
GROUPBYARREST = group FILTERBYARREST by District;

ARRESTCOUNTBYDISTRICT = foreach GROUPBYARREST generate group as District,COUNT(FILTERBYARREST) as ArrestCount;

store ARRESTCOUNTBYDISTRICT into '/user/acadgild/project/1.3_arrest_count_by_district';
