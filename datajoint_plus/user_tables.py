"""
Hosts the original DataJoint table tiers extended with DataJointPlus.
"""
import datajoint as dj

from .base import MasterBase, PartBase


class Lookup(MasterBase, dj.Lookup):
    pass


class Manual(MasterBase, dj.Manual):
    pass


class Computed(MasterBase, dj.Computed):
    pass


class Imported(MasterBase, dj.Imported):
    pass


class Part(PartBase, dj.Part):
    pass
