# Databricks notebook source
from workflows.sequential_workflow import SequentialWorkflow
from workflows.task import Task

# COMMAND ----------

flow = SequentialWorkflow("my_workflow")

Task(flow,
     "task1",
     [{"whl": "dbfs:/databricks/jars/DatabricksJDBC42.whl"}],
     "package",
     "entry_point",
     "a",
     "b"
    )

Task(flow,
     "task2",
     [{"whl": "dbfs:/databricks/jars/DatabricksJDBC42.whl"}],
     "package",
     "entry_point",
     "c",
     "d"
    )

Task(flow,
     "task3",
     [{"whl": "dbfs:/databricks/jars/DatabricksJDBC42.whl"}],
     "package2",
     "entry_point2",
     "c1",
     "d1"
    )

# COMMAND ----------

flow.run()
