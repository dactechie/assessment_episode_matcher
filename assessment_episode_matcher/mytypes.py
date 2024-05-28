from enum import Enum, auto
from dataclasses import dataclass, fields
from typing import Any, Optional #, KW_ONLY
from collections import namedtuple


@dataclass
class CSVTypeObject:
  header:list[str]
  rows:list

@dataclass
class AODWarning:
  SLK:str
  RowKey:str
  drug_name:str
  field_name:str
  field_value:Optional[str]=""
   
  #  def __str__(self):
  #   return f"{self.SLK},{self.RowKey},{self.drug_name},{self.field_name}"
  def to_list(self) -> list[str]:
    f = [getattr(self, field.name) for field in fields(self)]
    return f
    # field_values = []
    # for field in fields(self):
    #     value = getattr(self, field.name)
    #     field_values.append(value)
    # return field_values
  
class DataType(Enum):
  ASSESSMENTS = auto()
  EPISODES = auto()
  PROCESSED_ASSESSMENTS = auto()
  PROCESSED_EPISODES = auto()
  # OTHER = auto()

class Purpose(Enum):
  NADA = 1
  MATCHING = 2


class ResultType(Enum):
  OK = auto()
  NOT_OK = auto()


class DataKeys(Enum):
  client_id =  'SLK'
  episode_id = 'PMSEpisodeID'
  per_client_asmt_id = 'RowKey'
  assessment_id = f"{client_id}_{per_client_asmt_id}"  #'SLK_RowKey'
  assessment_date = 'AssessmentDate'
  episode_start_date = 'CommencementDate'
  episode_end_date = 'EndDate'


class IssueLevel(Enum):
  WARNING = auto()
  ERROR = auto()

class IssueType(Enum):
  DATE_MISMATCH = auto()        #1
  SLKPROG_ONLY_IN_ASSESSMENT = auto()
  CLIENT_ONLYIN_ASMT = auto()   #3
  SLKPROG_ONLY_IN_EPISODE = auto()
  CLIENT_ONLYIN_EPISODE = auto()#5
  ASMT_MATCHED_MULTI = auto() 
  NO_ASMT_IN_EPISODE = auto()   #7
  INPERIOD_ASMTSLK_NOTIN_EP = auto()
  # inperiod_atomslk_notin_ep


@dataclass()
class ValidationIssue(Exception):  
  msg:str
  issue_type:IssueType
  issue_level:IssueLevel
  key:Optional[str] = None

  def make_copy(self):
    return ValidationIssue(self.msg, self.issue_type,self.issue_level, self.key)
  
  # def to_dict(self):
  #     return {
  #         "msg": self.msg,
  #         "issue_type": self.issue_type.name,
  #         "issue_level": self.issue_level.name,
  #         "key": self.key
  #     }

@dataclass(kw_only=True)
class ValidationError(ValidationIssue):
  issue_level:IssueLevel= IssueLevel.ERROR


@dataclass(kw_only=True)
class ValidationWarning(ValidationIssue):
  issue_level:IssueLevel= IssueLevel.WARNING

ValidationMaskIssueTuple = namedtuple('ValidationIssue', ['mask', 'validation_issue'])