from enum import Enum, auto
from dataclasses import dataclass
from typing import Optional #, KW_ONLY
from collections import namedtuple


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

class DatasetType(Enum):
    ASSESSMENT = 'assessment'
    EPISODE = 'episode'


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