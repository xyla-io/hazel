from datetime import datetime, date
from typing import Dict

class GoogleAdsQuery:
  query: str
  parameters: Dict[str, any]

  def __init__(self, query: str, parameters: Dict[str, any]={}):
    self.query = query
    self.parameters = parameters

  @property
  def query_text(self) -> str:
    escaped_parameters = {
      k: type(self).format_parameter(v)
      for k, v in self.parameters.items()
    }
    return self.query.format(**escaped_parameters)

  @classmethod
  def format_parameter(cls, parameter: any, format_list: bool=True) -> str:
    if type(parameter) is date or type(parameter) is datetime:
      return parameter.strftime("'%Y-%m-%d'")
    elif type(parameter) is list and format_list:
      return '( ' + ', '.join([cls.format_parameter(p, format_list=False) for p in parameter]) + ' )'
    else:
      return "'" + str(parameter).replace("'", "\\'") + "'"
