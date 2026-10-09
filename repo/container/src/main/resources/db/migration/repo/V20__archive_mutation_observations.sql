-- Mutable physical observations are separate from immutable logical admission.
CREATE TABLE archive_mutation_observations (
    account_id varchar(200) NOT NULL,
    principal varchar(200) NOT NULL,
    operation_id uuid NOT NULL,
    status_revision bigint NOT NULL CHECK (status_revision > 0),
    receipt bytea NOT NULL,
    PRIMARY KEY(account_id,principal,operation_id),
    FOREIGN KEY(account_id,principal,operation_id) REFERENCES archive_mutations(account_id,principal,operation_id)
);
