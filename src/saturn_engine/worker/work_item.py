from .executable_message import ExecutableMessage
from .scheduler import Schedulable


class WorkItem(Schedulable[ExecutableMessage]):
    pass
