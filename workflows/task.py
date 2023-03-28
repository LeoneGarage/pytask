class Task:
  _workflow = None
  _name = None
  _libraries = None
  _package = None
  _entry_point = None
  _cluster_name = None
  _parameters = []
  _depends_on = []

  def __init__(self, workflow, name, libraries, package, entry_point, *parameters):
    self._workflow = workflow
    self._name = name
    self._libraries = libraries
    self._package = package
    self._entry_point = entry_point
    self._parameters = parameters
    cluster = workflow.get_cluster(name)
    if cluster is not None:
      self._cluster_name = cluster.name
    else:
      self._cluster_name = "default"
    workflow._append(self)
  
  @property
  def name(self):
    return self._name

  @property
  def libraries(self):
    return self._libraries

  @property
  def package(self):
    return self._package

  @property
  def entry_point(self):
    return self._entry_point

  @property
  def parameters(self):
    return self._parameters

  @property
  def cluster_name(self):
    return self._cluster_name
  
  @cluster_name.setter
  def cluster_name(self, value):
    self._cluster_name = value
    
  @property
  def dependencies(self):
    return self._depends_on

  def depends_on(self, *depends_on):
    self._depends_on = self._depends_on + list(depends_on)
    return self