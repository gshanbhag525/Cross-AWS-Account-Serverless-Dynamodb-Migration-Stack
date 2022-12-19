import aws_cdk as core
import aws_cdk.assertions as assertions

from pipeline_stack.pipeline_stack_stack import PipelineStackStack

# example tests. To run these tests, uncomment this file along with the example
# resource in epilite_pipeline_stack/epilite_pipeline_stack_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = PipelineStackStack(app, "pipeline-stack")
    template = assertions.Template.from_stack(stack)

