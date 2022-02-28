"""
Exception classes for the DataJointPlus
"""
from datajoint.errors import DataJointError


class ValidationError(DataJointError):
    """
    Validation failure.
    """

class OverwriteError(DataJointError):
    """
    Overwrite not permissible.
    """