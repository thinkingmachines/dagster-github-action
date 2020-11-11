import sys
import weakref

import requests
from dagster import (
    DagsterError,
    DagsterInvariantViolationError,
    EventMetadataEntry,
    Field,
    StringSource,
    check,
)
from dagster.cli.api import ExecuteRunArgs
from dagster.core.events import EngineEventData
from dagster.core.host_representation import ExternalPipeline
from dagster.core.host_representation.handle import GrpcServerRepositoryLocationHandle
from dagster.core.instance import DagsterInstance
from dagster.core.launcher import RunLauncher
from dagster.core.origin import PipelineGrpcServerOrigin, PipelinePythonOrigin
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus
from dagster.serdes import (
    ConfigurableClass,
    ConfigurableClassData,
    serialize_dagster_namedtuple,
)
from dagster.utils.error import serializable_error_info_from_exc_info


class GHActionRunLauncher(RunLauncher, ConfigurableClass):
    def __init__(
        self,
        token,
        inst_data=None,
    ):
        self._inst_data = check.opt_inst_param(
            inst_data, "inst_data", ConfigurableClassData
        )
        self.token = check.str_param(token, "token")
        self._instance_ref = None

    @classmethod
    def config_type(cls):
        return {
            "token": Field(StringSource, is_required=True),
        }

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    @property
    def inst_data(self):
        return self._inst_data

    @property
    def _instance(self):
        return self._instance_ref() if self._instance_ref else None

    def initialize(self, instance):
        check.inst_param(instance, "instance", DagsterInstance)
        # Store a weakref to avoid a circular reference / enable GC
        self._instance_ref = weakref.ref(instance)

    def launch_run(self, instance, run, external_pipeline):
        check.inst_param(run, "run", PipelineRun)
        check.inst_param(external_pipeline, "external_pipeline", ExternalPipeline)

        pipeline_origin = None
        dispatch_url = None
        ref = None

        if isinstance(external_pipeline.get_origin(), PipelineGrpcServerOrigin):
            repository_location_handle = (
                external_pipeline.repository_handle.repository_location_handle
            )

            if not isinstance(
                repository_location_handle, GrpcServerRepositoryLocationHandle
            ):
                raise DagsterInvariantViolationError(
                    "Expected RepositoryLocationHandle to be of type "
                    "GrpcServerRepositoryLocationHandle but found type {}".format(
                        type(repository_location_handle)
                    )
                )

            repository_name = external_pipeline.repository_handle.repository_name
            pipeline_origin = PipelinePythonOrigin(
                pipeline_name=external_pipeline.name,
                repository_origin=repository_location_handle.reload_repository_python_origin(
                    repository_name
                ),
            )

            # HACK
            dispatch_url, ref = repository_location_handle.get_current_image().split("@")
        else:
            raise DagsterError("Pipeline origin needs to be a GRPC server")

        input_json = serialize_dagster_namedtuple(
            ExecuteRunArgs(
                pipeline_origin=pipeline_origin,
                pipeline_run_id=run.run_id,
                instance_ref=None,
            )
        )

        resp = requests.post(
            f"https://api.github.com/{dispatch_url}",
            json={"ref": ref, "inputs": {"input_json": input_json}},
            headers={
                "Authorization": f"token {self.token}",
                "Accept": "application/vnd.github.v3+json",
            },
        )
        resp.raise_for_status()

        self._instance.report_engine_event(
            "GitHub Action workflow dispatched",
            run,
            EngineEventData(
                [
                    EventMetadataEntry.text(dispatch_url, "Dispatch URL"),
                    EventMetadataEntry.text(ref, "Ref"),
                ]
            ),
            cls=self.__class__,
        )

        return run

    # https://github.com/dagster-io/dagster/issues/2741
    def can_terminate(self, run_id):
        check.str_param(run_id, "run_id")

        pipeline_run = self._instance.get_run_by_id(run_id)
        if not pipeline_run:
            return False
        if pipeline_run.status != PipelineRunStatus.STARTED:
            return False
        return True

    def terminate(self, run_id):
        check.str_param(run_id, "run_id")
        run = self._instance.get_run_by_id(run_id)

        if not run:
            return False

        self._instance.report_engine_event(
            message="Received pipeline termination request.",
            pipeline_run=run,
            cls=self.__class__,
        )

        can_terminate = self.can_terminate(run_id)
        if not can_terminate:
            self._instance.report_engine_event(
                message="Unable to terminate pipeline; can_terminate returned {}".format(
                    can_terminate
                ),
                pipeline_run=run,
                cls=self.__class__,
            )
            return False

        try:
            termination_result = None
            if termination_result:
                self._instance.report_engine_event(
                    message="Pipeline was terminated successfully.",
                    pipeline_run=run,
                    cls=self.__class__,
                )
            else:
                self._instance.report_engine_event(
                    message="Pipeline was not terminated successfully; delete_job returned {}".format(
                        termination_result
                    ),
                    pipeline_run=run,
                    cls=self.__class__,
                )
            return termination_result
        except Exception:  # pylint: disable=broad-except
            self._instance.report_engine_event(
                message="Pipeline was not terminated successfully; encountered error in delete_job",
                pipeline_run=run,
                engine_event_data=EngineEventData.engine_error(
                    serializable_error_info_from_exc_info(sys.exc_info())
                ),
                cls=self.__class__,
            )
