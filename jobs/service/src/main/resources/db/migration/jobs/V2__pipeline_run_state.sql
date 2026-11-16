-- A pipeline run stores its state as protobuf: the definition it executes, its
-- ordered step checkpoints and its result. A workflow run keeps its JSON
-- columns. A row is one or the other, never both.

ALTER TABLE workflow_run
    -- A serialized ai.protomolt.proto.pipeline.v1.PipelineRunDefinition.
    ADD COLUMN pipeline_definition_pb BYTEA,
    -- A serialized ai.protomolt.proto.pipeline.v1.PipelineRunCheckpoints.
    ADD COLUMN pipeline_checkpoints_pb BYTEA,
    -- A serialized ai.protomolt.proto.pipeline.v1.PipelineRunResult; set on COMPLETED only.
    ADD COLUMN pipeline_result_pb BYTEA,
    ALTER COLUMN workflow_definition DROP NOT NULL,
    ALTER COLUMN input DROP NOT NULL,
    ALTER COLUMN checkpoints DROP NOT NULL;

ALTER TABLE workflow_run
    ADD CONSTRAINT chk_workflow_run_one_definition CHECK (
        (workflow_definition IS NOT NULL AND input IS NOT NULL AND checkpoints IS NOT NULL
            AND pipeline_definition_pb IS NULL AND pipeline_checkpoints_pb IS NULL
            AND pipeline_result_pb IS NULL)
        OR
        (pipeline_definition_pb IS NOT NULL
            AND workflow_definition IS NULL AND input IS NULL AND checkpoints IS NULL
            AND result IS NULL));
