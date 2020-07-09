from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators

# Defining the plugin class
class MyPlugin(AirflowPlugin):
    name = "myplugin"
    # Define your operators in this plugin
    operators = [
        operators.StateToS3Operator,
        operators.stage_gcs_operator,
        operators.googletrend_operator
    ]

