"""Orchestration Stack - Step Functions and EventBridge for EVM Pipeline."""

from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct

from .lambda_stack import LambdaStack
from .storage_stack import StorageStack


class OrchestrationStack(Stack):
    """Stack containing Step Functions state machine and EventBridge schedule."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        lambda_stack: LambdaStack,
        storage_stack: StorageStack,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Step 1: Fetch Latest Block
        fetch_block_task = tasks.LambdaInvoke(
            self,
            "FetchLatestBlock",
            lambda_function=lambda_stack.fetch_latest_block_fn,
            output_path="$.Payload",
            comment="Fetch latest block for each chain with reorg buffer",
        )

        # Add retry configuration for fetch block
        fetch_block_task.add_retry(
            errors=["States.TaskFailed", "Lambda.ServiceException"],
            interval=Duration.seconds(10),
            max_attempts=3,
            backoff_rate=2,
        )

        # Add catch for fetch block failures
        fetch_block_fail = sfn.Fail(
            self,
            "FetchBlockFailed",
            error="FetchBlockError",
            cause="Failed to fetch latest block after retries",
        )
        fetch_block_task.add_catch(fetch_block_fail, errors=["States.ALL"])

        # Check if there are contracts to process
        check_contracts = sfn.Choice(self, "CheckContracts")

        no_contracts = sfn.Succeed(
            self,
            "NoContractsFound",
            comment="No contracts registered to process",
        )

        # Step 2: Sync Raw Data (Map state for parallel contract processing)
        sync_raw_task = tasks.LambdaInvoke(
            self,
            "SyncRawData",
            lambda_function=lambda_stack.sync_raw_data_fn,
            output_path="$.Payload",
            comment="Sync raw event logs from Etherscan to DeltaLake",
        )

        # Add retry for sync task
        sync_raw_task.add_retry(
            errors=["States.TaskFailed", "Lambda.ServiceException"],
            interval=Duration.seconds(30),
            max_attempts=3,
            backoff_rate=2,
        )

        # Map state for syncing multiple contracts in parallel
        sync_map = sfn.Map(
            self,
            "SyncContractsMap",
            items_path="$.contracts",
            max_concurrency=5,
            comment="Process each contract in parallel (max 5 concurrent)",
        )
        sync_map.item_processor(sync_raw_task)

        # Step 3: Decode Data (Map state for parallel decoding)
        decode_task = tasks.LambdaInvoke(
            self,
            "DecodeData",
            lambda_function=lambda_stack.decode_data_fn,
            output_path="$.Payload",
            comment="Decode raw event logs using contract ABIs",
        )

        # Add retry for decode task
        decode_task.add_retry(
            errors=["States.TaskFailed", "Lambda.ServiceException"],
            interval=Duration.seconds(30),
            max_attempts=3,
            backoff_rate=2,
        )

        # Map state for decoding
        decode_map = sfn.Map(
            self,
            "DecodeContractsMap",
            items_path="$",
            max_concurrency=5,
            comment="Decode each contract's logs in parallel (max 5 concurrent)",
        )
        decode_map.item_processor(decode_task)

        # Success state
        success = sfn.Succeed(
            self,
            "PipelineSuccess",
            comment="EVM Pipeline sync completed successfully",
        )

        # Chain the steps together
        # fetch -> check if contracts exist -> sync map -> decode map -> success
        definition = fetch_block_task.next(
            check_contracts.when(
                sfn.Condition.or_(
                    sfn.Condition.not_(
                        sfn.Condition.is_present("$.contracts")
                    ),
                    sfn.Condition.number_equals("$.contracts[0]", 0),
                ),
                no_contracts,
            ).otherwise(
                sync_map.next(decode_map).next(success)
            )
        )

        # Create State Machine
        self.state_machine = sfn.StateMachine(
            self,
            "EvmPipelineStateMachine",
            state_machine_name="evm-pipeline-sync",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(1),
            tracing_enabled=True,
            comment="EVM Pipeline data synchronization state machine",
        )

        # EventBridge Rule: Every 30 minutes
        schedule_rule = events.Rule(
            self,
            "ScheduleRule",
            rule_name="evm-pipeline-schedule",
            schedule=events.Schedule.rate(Duration.minutes(30)),
            description="Triggers EVM Pipeline sync every 30 minutes",
            enabled=True,
        )

        # Add Step Function as target
        schedule_rule.add_target(
            targets.SfnStateMachine(
                self.state_machine,
                input=events.RuleTargetInput.from_object(
                    {
                        "triggered_at": events.EventField.from_path("$.time"),
                        "source": "scheduled",
                    }
                ),
            )
        )

        # Outputs
        CfnOutput(
            self,
            "StateMachineArn",
            value=self.state_machine.state_machine_arn,
            description="ARN of the EVM Pipeline State Machine",
        )

        CfnOutput(
            self,
            "StateMachineName",
            value=self.state_machine.state_machine_name,
            description="Name of the EVM Pipeline State Machine",
        )

        CfnOutput(
            self,
            "ScheduleRuleName",
            value=schedule_rule.rule_name,
            description="Name of the EventBridge schedule rule",
        )
