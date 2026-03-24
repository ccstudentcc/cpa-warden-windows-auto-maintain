"""Compatibility wrapper; canonical module moved to subpackage."""

from importlib import import_module as _import_module
import sys as _sys

_sys.modules[__name__] = _import_module("cwma.auto.channel.channel_feedback")
