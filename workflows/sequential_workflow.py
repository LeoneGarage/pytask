from workflows.workflow import Workflow

class SequentialWorkflow(Workflow):
  def __init__(self, name):
    super().__init__(name)
  
  def _append(self, task):
    lastTask = self._getLastTask()
    if lastTask is not None:
      task = task.depends_on(lastTask)
    super()._append(task)