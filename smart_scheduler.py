import sys

from cwma.scheduler import smart_scheduler as _impl

sys.modules[__name__] = _impl
