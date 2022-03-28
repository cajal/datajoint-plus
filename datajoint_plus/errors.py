"""
Exception classes for the DataJointPlus
"""
from datajoint.errors import DataJointError


class DataJointPlusError(DataJointError):
    pass


class ValidationError(DataJointPlusError):
    """
    Validation failure.
    """

class OverwriteError(DataJointPlusError):
    """
    Overwrite not permissible.
    """

class MotifError(DataJointPlusError):
    pass


class MakerError(MotifError):
    pass


class MakerInputError(MotifError):
    pass


class MakerMethodError(MotifError):
    pass


class MakerDestinationError(MotifError):
    pass