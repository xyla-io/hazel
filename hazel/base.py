import functools

from google.ads.google_ads.errors import GoogleAdsException
from typing import Optional

def print_ga_error():
  def wrap(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
      try:
        return f(*args, **kwargs)
      except GoogleAdsException as e:
        print('Request with ID "%s" failed with status "%s" and includes the '
                'following errors:' % (e.request_id, e.error.code().name))
        for error in e.failure.errors:
          print('\tError with message "%s".' % error.message)
          if error.location:
            for field_path_element in error.location.field_path_elements:
              print('\t\tOn field: %s' % field_path_element.field_name)
        raise e
    return wrapper
  return wrap

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