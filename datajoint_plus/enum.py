"""
Enum classes.
"""

from enum import Enum


class JoinMethod(Enum):
    PRIMARY = 'primary_only'
    SECONDARY = 'rename_secondaries'
    COLLISIONS = 'rename_collisions'
    ALL = 'rename_all'
