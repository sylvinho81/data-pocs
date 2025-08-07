CALL delta.system.flush_metadata_cache();

CREATE SCHEMA IF NOT EXISTS delta.default WITH (location = 's3a://delta-lake/');



-- Option 1: Register table using the system procedure
CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'users',
    table_location => 's3a://delta-lake/users'
);


CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'clients',
    table_location => 's3a://delta-lake/clients'
);

CALL delta.system.flush_metadata_cache();
