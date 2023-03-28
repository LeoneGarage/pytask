class Cluster:
  _name = None
  _config = None

  def __init__(self, workflow, name, config):
    self._name = name
    self._config = config
    workflow._add_cluster(self)
  
  @property
  def name(self):
    return self._name
  
  @property
  def config(self):
    return self._config