#!/usr/bin/env python3
import os
import aws_cdk as cdk

from pipeline_stack.pipeline_stack_stack import (
    PipelineStackStack,
)

app = cdk.App()
PipelineStackStack(app, "PipelineStackStack")
app.synth()
