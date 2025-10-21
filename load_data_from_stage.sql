/*
CALL COMMON_DATA.load_data_from_stage(
    table_name => 'ALL_TABLES',
    stage_name => 'ETL_S3_STAGE_DATA',
    file_format_name => 'csv_format',
    database_name => 'SALES_DB',
    schema_name => 'RAW_DATA'
);
*/

CREATE OR REPLACE PROCEDURE COMMON_DATA.load_data_from_stage(
    table_name VARCHAR DEFAULT 'ALL_TABLES', -- Default to loading all tables
    stage_name VARCHAR DEFAULT 'ETL_S3_STAGE_DATA', -- Stage name parameter
    file_format_name VARCHAR DEFAULT 'csv_format',     -- File format parameter
    database_name VARCHAR DEFAULT NULL, -- Database name parameter
    schema_name VARCHAR DEFAULT NULL    -- Schema name parameter
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    // List of the six target tables based on the provided schema
    var ALL_TABLES = [
        "MANUFACTURERS", "PRODUCTS", "CUSTOMERS", "INVENTORY", "SALES",
        "SALES_LINE_ITEMS"
    ];

    var results = [];
    var tablesToLoad = [];
    var inputTableName = TABLE_NAME ? TABLE_NAME.toUpperCase() : 'ALL_TABLES';

    // Helper function to build the fully qualified table name (e.g., DB.SCHEMA.TABLE)
    function getQualifiedTableName(dbName, schemaName, tableName) {
        let name = '';
        if (dbName) {
            name += dbName + '.';
        }
        if (schemaName) {
            name += schemaName + '.';
        }
        // Table names from ALL_TABLES are already uppercase
        name += tableName;
        return name;
    }

    // 1. Determine which tables to load
    if (inputTableName === 'ALL_TABLES') {
        tablesToLoad = ALL_TABLES;
        results.push("Mode: Attempting to load ALL tables: " + ALL_TABLES.join(", "));
        if (DATABASE_NAME || SCHEMA_NAME) {
            results.push("Target Schema Prefix: " + getQualifiedTableName(DATABASE_NAME, SCHEMA_NAME, '') + " (using NULL for current context if missing)");
        }
    } else if (ALL_TABLES.includes(inputTableName)) {
        // Since ALL_TABLES are uppercase, direct check is fine
        var matchedTable = ALL_TABLES.find(t => t === inputTableName);
        tablesToLoad = [matchedTable];
        results.push("Mode: Attempting to load single table: " + matchedTable);
    } else {
        return "ERROR: Invalid table name provided ('" + TABLE_NAME + "'). Supported tables are: " + ALL_TABLES.join(", ") + " or use 'ALL_TABLES'.";
    }

    // 2. Execute COPY INTO statement for each selected table
    // NOTE: The load order is crucial due to Foreign Key dependencies: 
    // MANUFACTURERS -> PRODUCTS -> INVENTORY, SALES -> SALES_LINE_ITEMS.
    // However, since Snowflake COPY INTO doesn't enforce FKs, the order mostly matters 
    // for logical processing. We'll stick to the explicit list order.
    for (var i = 0; i < tablesToLoad.length; i++) {
        var tableName = tablesToLoad[i];

        // Get the fully qualified table name
        var qualifiedTableName = getQualifiedTableName(DATABASE_NAME, SCHEMA_NAME, tableName);

        // Construct the COPY INTO SQL statement
        // Assumes file name on stage matches the table name (e.g., @STAGE/MANUFACTURERS.csv)
        var copy_sql = `
            COPY INTO ${qualifiedTableName}
            FROM @${STAGE_NAME}/${tableName.toLowerCase()}.csv -- Use lowercase .csv for convention
            FILE_FORMAT = (FORMAT_NAME = '${FILE_FORMAT_NAME}')
            ON_ERROR = 'ABORT_STATEMENT' -- Fail the copy operation immediately on the first error
            --PURGE = TRUE; -- Remove the file from the stage after successful load
        `;

        try {
            var stmt = snowflake.createStatement({ sqlText: copy_sql });
            var res = stmt.execute();
            var load_details = [];

            // Extract key details from the result set
            if (res.next()) {
                load_details.push(`Rows loaded: ${res.getColumnValue('rows_loaded')}`);
                load_details.push(`Status: ${res.getColumnValue('status')}`);
            }

            results.push(`SUCCESS: Data loaded into ${qualifiedTableName}. Details: ${load_details.join(', ')}.`);

        } catch (err) {
            results.push(`FAILURE: Failed to load data into ${qualifiedTableName}. Error: ${err.message}.`);
            results.push(`SQL executed: ${copy_sql.trim()}`);
            // If running 'ALL_TABLES' mode, a failure on a single table should be noted, but the loop continues.
        }
    }

    // 3. Return the consolidated results
    return results.join('\n');
$$;