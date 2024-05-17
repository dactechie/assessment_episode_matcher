from pandas.api.types import CategoricalDtype
from assessment_episode_matcher.mytypes import Purpose


keep_parent_fields = ['Program', 'Staff', 'AssessmentDate', 'SLK'] # if also in SurveyData 
# {
#   'Matching':['Program', 'Staff']
# }

ATOM_DB_filters = {
  # extract purpose
   Purpose.NADA : {
    'Program' : ('SAPPHIRE','EUROPATH','MONPATH','BEGAPATH'
                 ,'MURMICE','GOLBGNRL','GOLBICE','MURMPP','MURMWIO','MURMHEAD',) 
   },
   Purpose.MATCHING:{}
   
  }
# range
# remove_if_under_threshold = {
#      'PDCHowMuchPerOccasion' : {
#           'Other': 2 # if under 2 percent of dataset, delete records
#      }
# }
category_yes_no = CategoricalDtype(['Yes', 'No'])

# TODO Fix:
# Past4WkBeenArrested :'No', 'Yes', 'Yes - please provide details'

# df.Past4WkAodRisks[df.Past4WkAodRisks.notna()]
# 3       [Driving with drugs and/or alcohol in your sys...
# 8                    [Using more than one drug at a time]
# 311       [Memory Loss, Using Alone, Violence / Assault]
# 7312                                        [Using Alone]
# 7314    [Using Alone, Driving with drugs and/or alcoho...
# HaveAnySocialSupport
# Some            1698
# A few           1583
# Quite a lot     1181
# A wide range     668
# None             276

category_feel_safe = CategoricalDtype(['Yes - Completely safe',
       'Mostly safe. Sometimes feel threatened',
       'Often feel unsafe / Occasionally experience violence',
       'Never feel safe / Constantly exposed to violence'      ])

category_notatall_to_daily = CategoricalDtype(['Not at all'
                                               ,  'Less than weekly'
                                               , 'Once or twice per week'
                                               , 'Three or four times per week'
                                               , 'Daily or almost daily'], ordered=True)

# HowSatisfiedWithProgress
category_notatall_to_extremely = CategoricalDtype([
        'Not at all ' , 'Slightly' , 'Moderately'
        , 'Considerably' , 'Extremely'], ordered=True)


predef_categories = {
    'Past4WkDailyLivingImpacted': category_notatall_to_daily,
    'Past4WkUseLedToProblemsWithFamilyFriend':category_notatall_to_daily,
  'Past4WkHowOftenIllegalActivities':       category_notatall_to_daily,
  'Past4WkHowOftenMentalHealthCausedProblems': category_notatall_to_daily,
  'Past4WkHowOftenPhysicalHealthCausedProblems': category_notatall_to_daily, 
  'Past4WkDifficultyFindingHousing': category_notatall_to_daily,
  'HowSatisfiedWithProgress': category_notatall_to_extremely,
  'DoYouFeelSafeWhereYouLive': category_feel_safe,
  'Past4WkHadCaregivingResponsibilities': category_yes_no,
  'Past4WkBeenHospCallAmbulance': category_yes_no,
  'Past4WkAnyOtherAddictiveB': category_yes_no,
}

question_list_for_categories = [
  'Program',
  'Staff',
  'SurveyName',
  'AssessmentType',

  'IndigenousStatus',
  'ClientType',
  'CountryOfBirth',
  'LivingArrangement',

  # 'PDCSubstanceOrGambling',
  # 'PDCGoals',  
  # 'PDCMethodOfUse',

  'HowImportantIsChangeToYou',
  'HaveAnySocialSupport',  
]

question_list_for_categories = question_list_for_categories + list(predef_categories.keys())


# survey_datacols_preregex = [
#    '.*_Score$'
# ]
# survey_datacols_postregex = [
#    '^Past4Wk.*'
# ]

mulselect_option_to_nadafield = {
    'Past4WkAodRisks': {'ATOPRiskEviction': "At risk of eviction",
                         'ATOPHomeless': "Homeless",
                         'Past4Wk_ViolentToYou': "Violence / Assault"
                         },
    'PrimaryCaregiver': {
      'PrimaryCaregiver_0-5':'Yes - primary caregiver: children under 5 years old'  ,
      'PrimaryCaregiver_5-15': 'Yes - primary caregiver: children 5 - 15 years old',

    },
    
    'Past4WkEngagedInOtheractivities.Paid Work': { 'PaidWorkDays' :'Days'}, 
      'Past4WkEngagedInOtheractivities.Study - college, school or vocational education':{'StudyDays':'Days'}

  }

fieldname_suffixes_range= ["PerOccassionUse"]

# replace left(key) value with the right value in dataset
option_variants = {
    'PDCMethodOfUse': {
      'Ingests': 'Ingest',
      'Injects': 'Inject',
      'Smokes': 'Smoke',
    },
    'Past4WkDailyLivingImpacted': {
        'Once or twice a week' : 'Once or twice per week',
        'Three or four times a week': 'Three or four times per week'
    },
    'DoYouFeelSafeWhereYouLive': {
       'Often feel unsafe. Occasionally experience violence': 'Often feel unsafe / Occasionally experience violence',
       'Never feel safe. Constantly exposed to violence': 'Never feel safe / Constantly exposed to violence'             
    }
}

data_types:dict = {
  'PartitionKey': 'string',
'RowKey': 'string',
'Program': 'string',
'Staff': 'string',
'AssessmentDate': 'date',
'SurveyName': 'string',

# 'PDCSubstanceOrGambling': 'string',
# 'PDCMethodOfUse': 'string',
# 'PDCDaysInLast28': 'numeric',
# 'PDCUnits': 'string',

# 'PDCHowMuchPerOccasion': 'range', # can be 'Other'/NaN/float/int/range -> convert to float64, exclude Other

'HaveYouEverInjected': 'string',
'SDSIsAODUseOutOfControl': 'numeric',
'SDSDoesMissingFixMakeAnxious': 'numeric',
'SDSHowMuchDoYouWorryAboutAODUse': 'numeric',
'SDSDoYouWishToStop': 'numeric',
'SDSHowDifficultToStopOrGoWithout': 'numeric',

'Past4WkPhysicalHealth': 'numeric',
'Past4WkMentalHealth': 'numeric',
'Past4WkQualityOfLifeScore': 'numeric',
}

# -------------------------- Results Grouping -------------------------

results_grouping = {
    "Wellbeing measures":{
        'questions': ['Past4WkPhysicalHealth', 'Past4WkMentalHealth', 'Past4WkQualityOfLifeScore'],
        'description':'Changes in average scores for "Past 4 weeks: Wellbeing measures"'
    },
    "Substance Use": {
        'questions': ['PDCHowMuchPerOccasion' , 'PDCDaysInLast28'],
        'description':'Changes in average scores for "Past 4 weeks: Substance use"'
    },
    "Problems in Life Domains": {
        'questions': ['Past4WkDailyLivingImpacted'
                     , 
                    'Past4WkHowOftenPhysicalHealthCausedProblems'
                    , 'Past4WkHowOftenMentalHealthCausedProblems'
                    , 'Past4WkUseLedToProblemsWithFamilyFriend'
                    , 'Past4WkDifficultyFindingHousing'
                    ], #Past4WkHowOftenIllegalActivities
        'description':'Changes in average scores for "Past 4 weeks: Use let to problems in various Life domains"'
    },
    "SDS and K10" : {
        'questions': ['SDS_Score', 'K10_Score'],
        'description':'Changes in average scores for "SDS & K10"'
    }
}


# --------------- Filters -----------------------------------

funder_program_grouping ={
    'Coordinaire' : ['SENSW-Pathways', 'SENSW-Sapphire'],
    'NSW Ministry of Health' : ['NSW Methamphetamine Support'],
    'Murrumbidgee PHN' : ['Murrumbidgee Pathways'],
    'ACT Health' : ['TSS', 'Arcadia']    
}

program_grouping = {
      'SENSW-Pathways':['EUROPATH','MONPATH','BEGAPATH','GOLBGNRL'],
      'SENSW-Sapphire':['SAPPHIRE'],
      'NSW Methamphetamine Support':['MURMICE', 'GOLBICE'],
      'Murrumbidgee Pathways' : ['MURMWIO', 'MURMPP','MURMHEAD'],
      'TSS': ['TSS'],
      'Arcadia': ['ARCA']
}


# ------------------- MDS Mapping  -----

# SurveyCode	ESTABLISHMENT IDENTIFIER
EstablishmentID_Program = {
    '820002000': 'TSS'
    ,'82A000004': 'ARCCOCO'
    ,'82A000004': 'ARCRESI'
    ,'12QQ03076': 'SAPPHIRE'
    ,'12QQ03062': 'EUROPATH'
    ,'12QQ03061': 'MONPATH'
    ,'12QQ03063': 'BEGAPATH'
    ,'13K034': 'MURMICE'
    ,'12QQ03022': 'GOLBGNRL'
    ,'13Q035': 'GOLBICE'
    ,'12KK03024': 'MURMPP'
    ,'12KK03025': 'MURMWIO'
    ,'12KK03023': 'MURMHEAD'
}


# ---- for NADA Drug mapping pivot

activities_w_days = {'Past4WkEngagedInOtheractivities.Paid Work': 'PaidWorkDays',
   'Past4WkEngagedInOtheractivities.Study - college, school or vocational education':'StudyDays',
    # 'Past4WkEngagedInOtheractivities.Looking after children'
  }

PDC_ODC_ATOMfield_names = {
    'ODC': {
      'drug_name' : 'OtherSubstancesConcernGambling',
      'used_in_last_4wks' : 'DaysInLast28',
      'per_occassion': 'HowMuchPerOccasion',
      'units': 'Units',
    },
    'PDC':{
      'drug_name' : 'PDCSubstanceOrGambling',
      'used_in_last_4wks' : 'PDCDaysInLast28',
      'per_occassion': 'PDCHowMuchPerOccasion',
      'units': 'PDCUnits',
    }
}
