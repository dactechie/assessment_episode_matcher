
# TODO: use NADA fields
# DU Heroin use number of days
# DU Other opioid use number of days
# DU Cannabis use number of days
# DU Cocaine use number of days
# DU Amphetamine use number of days
# DU Tranquilliser use number of days
# DU Another drug use number of days
# DU Alcohol use number of days


nada_cols_final = [
  'AgencyCode'
]

nada_cols = [
'Program' # AgencyCode
,'RowKey'  # -> episode ID
, 'PartitionKey'  # -> MDS ClientCode
,'AssessmentType' # initial -> SurveyStatge  = 0 (progress)

,'Staff' #-> # Debugging
,'AssessmentDate' #5. COMS admin date

,'SDSIsAODUseOutOfControl'      # Q6
,'SDSDoesMissingFixMakeAnxious'
,'SDSHowMuchDoYouWorryAboutAODUse'
,'SDSDoYouWishToStop'             #15    Q4/
,'SDSHowDifficultToStopOrGoWithout'  # 11     Q5. How difficult would you/did you find it to stop  

## DU 
# ,'Alcohol_DaysInLast28'
# ,'Cannabis_DaysInLast28'  # 77. ATOP 1B.3   Cannabis Total (past 4 weeks)
# ,'Heroin_DaysInLast28'
# ,'Other Opioids_DaysInLast28'
# ,'Cocaine_DaysInLast28'
# ,'Amphetamines_DaysInLast28'
# ,'Benzodiazepines_DaysInLast28'
# ,'Another Drug1_DaysInLast28'


,'K10Q01'  # 30 
,'K10Q02'
,'K10Q03'
,'K10Q04'
,'K10Q05'
,'K10Q06'
,'K10Q07'
,'K10Q08'
,'K10Q09'
,'K10Q10'
,'K10Q11'
,'K10Q12'
,'K10Q13'
,'K10Q14'  # 43    Q14. In the last four weeks, how often have physical health problems been the main cause of these feelings?

# ATOP 65 - 160
# 65. ATOP 1A.1 ATOP Alcohol Typical Qty String 50
,'Alcohol_TypicalQtyStr'
# # The number of standard drinks of alcohol ingested on a typical drinking day refer to standard drinks guide.
# # 0 to 999 along with a description, being ‘standard drinks’
# # 66  Alcohol Wk 4 - 69 - Wk1  ---- ignore
# # 70 Total Alcohol : The total number of days alcohol consumed in the past four weeks.
,'Alcohol_DaysInLast28'
# #71. ATOP 1A.4  ATOP Alcohol No Answer  ; # 0 asked -1 not asked

, 'Alcohol_PerOccassionUse' # for DU


# #72 # Cannabis Typical Qty 
# # The amount of cannabis consumed on a typical day of cannabis use in the past four weeks. 
# # Agree on a meaningful unit of measure with the client. Common units of measure may include ‘grams’, number of times used per days, 
# # or the monetary value of drugs consumed. Please use the same unit of measure for subsequent survey time points.
# # 72. ATOP 1B.1  Cannabis Typical Qty (string) ; 0 to 999 (plus description of units)
,'Cannabis_TypicalQtyStr'
,'Cannabis_DaysInLast28'  # 77. ATOP 1B.3   Cannabis Total (past 4 weeks)

,'Heroin_TypicalQtyStr'
,'Heroin_DaysInLast28'

,'Other Opioids_TypicalQtyStr'
,'Other Opioids_DaysInLast28'

,'Cocaine_TypicalQtyStr'
,'Cocaine_DaysInLast28'

, 'Amphetamines_TypicalQtyStr'
,'Amphetamines_DaysInLast28'

,'Benzodiazepines_TypicalQtyStr'
,'Benzodiazepines_DaysInLast28'

,'Another Drug1'
,'Another Drug1_TypicalQtyStr'
# # 123. ATOP 1H.ii.1 0 to 999 (plus description of units)
# # Other Substance 2 Typical Qty The average amount of Other Substance 2 used on a typical day during the past four weeks. 
# # Agree on a meaningful unit of measure with the client. Common units of measure may include ‘grams’, number of times used per days, 
# # or the monetary value of drugs consumed. Please use the same unit of measure for subsequent survey time points.
,'Another Drug1_DaysInLast28'

,'Another Drug2'
,'Another Drug2_TypicalQtyStr'
# # 123. ATOP 1H.ii.1 0 to 999 (plus description of units)
# # Other Substance 2 Typical Qty The average amount of Other Substance 2 used on a typical day during the past four weeks. 
# # Agree on a meaningful unit of measure with the client. Common units of measure may include ‘grams’, number of times used per days, 
# # or the monetary value of drugs consumed. Please use the same unit of measure for subsequent survey time points.
,'Another Drug2_DaysInLast28'


# # 131 ATOP 1K.1 Daily Tobacco Use Confirmation of the client’s use of tobacco
# # 132 ATOP 1K.2  Daily Tobacco Use Typical Qty The average amount of tobacco used on a typical day during the past four weeks.
,'Nicotine_TypicalQtyStr'  # TODO: WARNING :typical day  != occassion
,'Nicotine_DaysInLast28'

, 'Nicotine_PerOccassionUse' # for DU

,'Past4WkNumInjectingDays'  # 136 ATOP 1K.2   Injected Total The total number of days injected in the past four weeks.



# ,'Past4WkEngagedInOtheractivities.Paid Work'     # 143. ATOP 2A.2 ATOP Days Paid Work Total
# # ,'Past4WkEngagedInOtheractivities.Voluntary Work'
# ,'Past4WkEngagedInOtheractivities.Study - college, school or vocational education'   # 149. ATOP 2B.2 he total number of days of school or study in the past four weeks.
, "ATOPHomeless",	"ATOPRiskEviction",	"PrimaryCaregiver_0-5",	"PrimaryCaregiver_5-15"
,'Past4WkBeenArrested' # 155  ATOP 2F Has the client been arrested over the past four weeks?

# ,'Gambling_PerOccassionUse'  ->> Question: should this be "Other drug" or  excluded all togther ?
# ,'Gambling_DaysInLast28'  ->> Question: should this be "Other drug" or  excluded all togther ?

, 'PaidWorkDays'
, 'StudyDays'
# ,'PrimaryCaregiver_0-5', 'PrimaryCaregiver_5-15'

, 'Past4Wk_ViolentToYou' # 
 # Past4WkAodRisks  ["Violence / Assault",]  # Has anyone been violent (incl. domestic violence) towards the client in past four weeks?
              #  #156.  ATOP 2G  ATOP Violent To You Has anyone been violent (incl. domestic violence) towards the client in past four weeks?

,'Past4WkHaveYouViolenceAbusive'  # ATOM:"Have you used violence or been abusive towards anyone, over the last 4 weeks?", "Yes (risk assessment required)"
         ##"Have you used violence or been abusive towards anyone, over the last 4 weeks?",:  157 ATOP 2H Violent To Others Has the client been violent (incl. domestic violence) towards someone else in the past four weeks?

,'Past4WkMentalHealth' #158  ATOP 2 I Psychological Health Status Client’s rating of their psychological wellbeing in past four weeks (anxiety, depression, problems with emotions and feelings) 0=poor 10=good
,'Past4WkPhysicalHealth'  # 159. ATOP 2J Physical Health Status Client’s rating of their physical health in past 4 weeks (extent of physical symptoms and bothered by illness)
,'Past4WkQualityOfLifeScore'  # 160.  ATOP 2K Qual of life  
]

# -1 not answered/no answer
notanswered_defaults = [
  'ATOPInjectedUsedEquipment',           ##
  'ATOPDailyTobaccoUse',                 ##
  # 'YourCurrentHousing_Homeless',         ##
  # 'YourCurrentHousing_Atriskofeviction', ##



"ATOPHomeless",	"ATOPRiskEviction",	"PrimaryCaregiver_0-5",	"PrimaryCaregiver_5-15",			

"Past4WkBeenArrested",	"Past4Wk_ViolentToYou", "Past4WkHaveYouViolenceAbusive",

"Past4WkMentalHealth",	"Past4WkPhysicalHealth",	"Past4WkQualityOfLifeScore",	

]

nada_final_fields = [
 "AgencyCode","PMSEpisodeID",	"PMSPersonID",	"Stage",	"AssessmentDate",	

"PDCCode",# SDS 1.0
"SDSIsAODUseOutOfControl",	"SDSDoesMissingFixMakeAnxious",	"SDSHowMuchDoYouWorryAboutAODUse",
  	"SDSDoYouWishToStop",	"SDSHowDifficultToStopOrGoWithout",		
"2nd SDS 1.1",	"2nd SDS 1.2",	"2nd SDS 1.3",	"2nd SDS 1.4",	"2nd SDS 1.5",		
# DU ---
"Heroin_DaysInLast28",	"Other Opioids_DaysInLast28",	"Cannabis_DaysInLast28",	
  "Cocaine_DaysInLast28",	"Amphetamines_DaysInLast28", "Benzodiazepines_DaysInLast28",
  "Another Drug1_DaysInLast28","Alcohol_DaysInLast28","Alcohol_PerOccassionUse",
  "DUDrinkingmoreheavilynumberofdrinks","DUDrinkingmoreheavilynumberofdays",
  "Nicotine_DaysInLast28", "Nicotine_PerOccassionUse",

"K10Q01",	"K10Q02",	"K10Q03",	"K10Q04",	"K10Q05",	"K10Q06",	"K10Q07",	"K10Q08",	"K10Q09",	"K10Q10",		
"K10Q11",	"K10Q12",	"K10Q13",	"K10Q14",	

# blanks ---------------------------------------------------
"QoLRatequalityoflife",	"QoLRatehealth",	"QoLRateenergyforeverydaylife",	"QoLMoneytomeetneeds",
"QoLAbilitytoperformdailyactivities",	"QoLSatisfiedwithself",	"QoLSatisfiedwithpersonalrelationships",
	"QoLSatisfiedwithconditionsofyourlivingplace",  "QoLPrincipalsourceofincome(MDS)", 
  "QoLLivingarrangements(MDS)",	"QoLUsualaccommodation(MDS)",			
"QoLNumberofoccasionsarrested(BTOM)",  "QoLNumberofarrestsforrecentoffences(BTOM)",
  "BBVInjectingdruguse(BTOM)",	"BBVSharingofneedleandsyringe(BTOM)",
    	"BBVSharingotherinjectingequipment(BTOM)",	"BBVDrugoverdoses(BTOM)",		
"NDDoyousmoketobacco?",	"NDHowsoonafterwakingdoyousmokeyourfirstcigarette?",
    "NDHowmanycigarettessmokedonatypicalday?","NDwithdrawalsorcravingsexperienced?",			
#--------------------blanks

# -- ATOP
"Alcohol_TypicalQtyStr",	"ATOPAlcoholWk4",	"ATOPAlcoholWk3",	"ATOPAlcoholWk2",
      "ATOPAlcoholWk1",	"Alcohol_DaysInLast28",	"ATOPAlcoholNoAnswer",
"Cannabis_TypicalQtyStr",	"ATOPCannabisWk4",	"ATOPCannabisWk3",	"ATOPCannabisWk2",
      "ATOPCannabisWk1",	"Cannabis_DaysInLast28",	"ATOPCannabisNoAnswer",
"Amphetamines_TypicalQtyStr",	"ATOPAmphetamineWk4",	"ATOPAmphetamineWk3",	"ATOPAmphetamineWk2",
      "ATOPAmphetamineWk1",	"Amphetamines_DaysInLast28",	"ATOPAmphetamineNoAnswer",
"Benzodiazepines_TypicalQtyStr",	"ATOPBenzodiazepinesWk4",	"ATOPBenzodiazepinesWk3",	"ATOPBenzodiazepinesWk2",
    	"ATOPBenzodiazepinesWk1",	"Benzodiazepines_DaysInLast28",	"ATOPBenzodiazepinesNoAnswer",
"Heroin_TypicalQtyStr",	"ATOPHeroinWk4",	"ATOPHeroinWk3",	"ATOPHeroinWk2",
      "ATOPHeroinWk1",	"Heroin_DaysInLast28",	"ATOPHeroinNoAnswer",
"Other Opioids_TypicalQtyStr",	"ATOPOtherOpiodsWk4",	"ATOPOtherOpiodsWk3",	"ATOPOtherOpiodsWk2",
      "ATOPOtherOpiodsWk1",	"Other Opioids_DaysInLast28",	"ATOPOtherOpiodsNoAnswer",
"Cocaine_TypicalQtyStr",	"ATOPCocaineWk4",	"ATOPCocaineWk3",	"ATOPCocaineWk2",	
      "ATOPCocaineWk1",	"Cocaine_DaysInLast28",	"ATOPCocaineNoAnswer",
"Another Drug1",	"Another Drug1_TypicalQtyStr",	"ATOPOtherSubstance1Wk4",	"ATOPOtherSubstance1Wk3",	"ATOPOtherSubstance1Wk2",
    	"ATOPOtherSubstance1Wk1",	"Another Drug1_DaysInLast28",'Another Drug1_NoAnswer' ,

"Another Drug2",	"Another Drug2_TypicalQtyStr",	"ATOPOtherSubstance2Wk4",	"ATOPOtherSubstance2Wk3",	"ATOPOtherSubstance2Wk2",
    	"ATOPOtherSubstance2Wk1",	"Another Drug2_DaysInLast28",'Another Drug2_NoAnswer',

"ATOPDailyTobaccoUse",	"Nicotine_TypicalQtyStr",				##  TODO	

"ATOPInjectedWk4",	"ATOPInjectedWk3",	"ATOPInjectedWk2",	"ATOPInjectedWk1",
    	"Past4WkNumInjectingDays",	"ATOPInjectedNoAnswer",	"ATOPInjectedUsedEquipment",

"ATOPDaysPaidWorkWk4",	"ATOPDaysPaidWorkWk3",	"ATOPDaysPaidWorkWk2",	
      "ATOPDaysPaidWorkWk1",	"PaidWorkDays",	"ATOPDaysPaidWorkNoAnswer",	
"ATOPDaysEducationWk4",	"ATOPDaysEducationWk3",	"ATOPDaysEducationWk2",	
      "ATOPDaysEducationWk1",	"StudyDays",	"ATOPDaysEducationNoAnswer",

"ATOPHomeless",	"ATOPRiskEviction",	"PrimaryCaregiver_0-5",	"PrimaryCaregiver_5-15",			

"Past4WkBeenArrested",	"Past4Wk_ViolentToYou", "Past4WkHaveYouViolenceAbusive",

"Past4WkMentalHealth",	"Past4WkPhysicalHealth",	"Past4WkQualityOfLifeScore",				

]


# 12. 2nd SDS 1.1 SDS Drug use out of control against the Intake drug 
# 13. 2nd SDS 1.2 SDS Drug use missing anxious/worried against the intake drug) ???swapped field 
# 14. 2nd SDS 1.3 SDS Drug use worry about use???swapped field 
# 15. 2nd SDS 1.4 SDS Drug use wish stop 
# 16. 2nd SDS 1.5 SDS Drug use difficult to stop

# ,'PrimaryCaregiver'  # ? 153 ? ATOP 2Ei Primary Caregiver Under 5 Has the client at any time in the past four weeks, been a primary care giver for or living with any child/children aged under 5 years?
# ,'Past4WkEngagedInOtheractivities.Looking after children'  # ? 153 ? ATOP 2Ei Primary Caregiver Under 5 Has the client at any time in the past four weeks, been a primary care giver for or living with any child/children aged under 5 years?
#             #154  Primary Caregiver 5 to15 Has the client at any time in the past four weeks, been a primary care giver for or living with any child/children aged under 5 years?

# ,'Past4WkHadCaregivingResponsibilities' # 154. ATOP 2Eii  # 1 yes 0 no -1 not answered/no answer