
# add  issue_type and issue_level to all
additional_fields = ["issue_type"  , "issue_level"]

COLUMNS_AUDIT_DATES = [
  "ESTABLISHMENT IDENTIFIER"
  ,"PMSEpisodeID"
  ,"PMSPersonID"
  ,"SLK"
  ,"RowKey"
  ,"CommencementDate"
  ,"AssessmentDate"
  ,"EndDate"
  ,"Program"
  ,"Staff"
  ,"SurveyName"
  ,"days_from_start"
  ,"days_from_end"
  ,"min_days"
]

COLUMNS_AUDIT_EPKEY_CLIENT = [ 
"ESTABLISHMENT IDENTIFIER"
  , "PMSEpisodeID"
  , "PMSPersonID"
  , "CommencementDate"
  , "EndDate"
  , "SLK"
  , "Program"

]

COLUMNS_AUDIT_EPKEY_CLIENTPROG = [ 
 *COLUMNS_AUDIT_EPKEY_CLIENT, "SLK_Program"
]


COLUMNS_AUDIT_ASMTKEY = [ 
   "SLK"
  , "RowKey"
	, "AssessmentDate"
  , "Program"
	, "Staff"
	, "SurveyName"

]



COLUMNS_AUDIT_DATES.extend(additional_fields)
COLUMNS_AUDIT_EPKEY_CLIENT.extend(additional_fields)
COLUMNS_AUDIT_EPKEY_CLIENTPROG.extend(additional_fields)
COLUMNS_AUDIT_ASMTKEY.extend(additional_fields)