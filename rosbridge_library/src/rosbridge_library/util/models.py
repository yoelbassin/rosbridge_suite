from typing import Any, Dict, List
from pydantic import BaseModel, ValidationError, model_validator

from rosbridge_library.internal.exceptions import InvalidArgumentException, MissingArgumentException

def get_missing_error(errors: List[Dict[str, Any]]):
    for error in errors:
        if error['type'] == 'missing':
            return error


class CapabilityBaseModel(BaseModel):    
    @model_validator(mode="wrap")
    def validate(self, handler):
        try:
            validated_self = handler(self)
        except ValidationError as e:
            missing_error = get_missing_error(e.errors())
            if missing_error:
                raise MissingArgumentException(f"Expected a {missing_error['loc'][0]} field but none was found.")
            else:
                raise InvalidArgumentException(
                    str(e)
                )
        return validated_self
