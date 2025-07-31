CALL delta.system.flush_metadata_cache();
CALL delta.system.sync_partition_metadata('default', 'users', 'FULL');
CALL delta.system.sync_partition_metadata('default', 'clients', 'FULL');


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
