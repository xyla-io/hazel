import functools

from google.ads.google_ads.errors import GoogleAdsException
from typing import Optional

def handle_ga_permission_error(default_value: Optional[any]=None):
  def wrap(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
      try:
        return f(*args, **kwargs)
      except GoogleAdsException as e:
        if str(e.error.code()) == 'StatusCode.PERMISSION_DENIED':
          return default_value
        raise e
    return wrapper
  return wrap