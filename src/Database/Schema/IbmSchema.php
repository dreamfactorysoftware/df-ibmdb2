<?php
namespace DreamFactory\Core\IbmDb2\Database\Schema;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\FunctionSchema;
use DreamFactory\Core\Database\Schema\ParameterSchema;
use DreamFactory\Core\Database\Schema\ProcedureSchema;
use DreamFactory\Core\Database\Schema\RoutineSchema;
use DreamFactory\Core\Database\Schema\Schema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbSimpleTypes;

/**
 * Schema is the class for retrieving metadata information from a IBM DB2 database.
 */
class IbmSchema extends Schema
{
    /**
     * @const string Quoting characters
     */
    const LEFT_QUOTE_CHARACTER = '"';

    const RIGHT_QUOTE_CHARACTER = '"';

    /**
     * @type boolean
     */
    private $isIseries = null;

    private function isISeries()
    {
        if ($this->isIseries !== null) {
            return $this->isIseries;
        }
        try {
            /** @noinspection SqlDialectInspection */
            $sql = 'SELECT * FROM QSYS2.SYSTABLES';
            $stmt = $this->connection->select($sql);
            $this->isIseries = (bool)$stmt;

            return $this->isIseries;
        } catch (\Exception $ex) {
            $this->isIseries = false;

            return $this->isIseries;
        }
    }

    protected function translateSimpleColumnTypes(array &$info)
    {
        // override this in each schema class
        $type = (isset($info['type'])) ? $info['type'] : null;
        switch (strtolower($type)) {
            // some types need massaging, some need other required properties
            case 'pk':
            case DbSimpleTypes::TYPE_ID:
                $info['type'] = 'integer';
                $info['allow_null'] = false;
                $info['auto_increment'] = true;
                $info['is_primary_key'] = true;
                break;

            case 'fk':
            case DbSimpleTypes::TYPE_REF:
                $info['type'] = 'integer';
                $info['is_foreign_key'] = true;
                // check foreign tables
                break;

            case DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE:
                $info['type'] = 'timestamp';
                $info['allow_null'] = false;
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (!isset($default)) {
                    $info['default'] = 'current timestamp';
                }
                break;
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE:
                $info['type'] = 'timestamp';
                $info['allow_null'] = false;
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (!isset($default)) {
                    $info['default'] = 'generated by default for each row on update as row change timestamp';
                } else {
                    $info['default'] = $default;
                }
                break;

            case DbSimpleTypes::TYPE_USER_ID:
            case DbSimpleTypes::TYPE_USER_ID_ON_CREATE:
            case DbSimpleTypes::TYPE_USER_ID_ON_UPDATE:
                $info['type'] = 'integer';
                break;

            case DbSimpleTypes::TYPE_DATETIME:
                $info['type'] = 'TIMESTAMP';
                break;

            case DbSimpleTypes::TYPE_FLOAT:
                $info['type'] = 'REAL';
                break;

            case DbSimpleTypes::TYPE_DOUBLE:
                $info['type'] = 'DOUBLE';
                break;

            case DbSimpleTypes::TYPE_MONEY:
                $info['type'] = 'decimal';
                $info['type_extras'] = '(19,4)';
                break;

            case DbSimpleTypes::TYPE_BOOLEAN:
                $info['type'] = 'smallint';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default)) {
                    // convert to bit 0 or 1, where necessary
                    $info['default'] = (int)filter_var($default, FILTER_VALIDATE_BOOLEAN);
                }
                break;

            case DbSimpleTypes::TYPE_STRING:
                $fixed =
                    (isset($info['fixed_length'])) ? filter_var($info['fixed_length'], FILTER_VALIDATE_BOOLEAN) : false;
                $national =
                    (isset($info['supports_multibyte'])) ? filter_var($info['supports_multibyte'],
                        FILTER_VALIDATE_BOOLEAN) : false;
                if ($fixed) {
                    $info['type'] = ($national) ? 'graphic' : 'character';
                } elseif ($national) {
                    $info['type'] = 'vargraphic';
                } else {
                    $info['type'] = 'varchar';
                }
                break;

            case DbSimpleTypes::TYPE_TEXT:
                $info['type'] = 'CLOB';
                break;

            case DbSimpleTypes::TYPE_BINARY:
                $fixed =
                    (isset($info['fixed_length'])) ? filter_var($info['fixed_length'], FILTER_VALIDATE_BOOLEAN) : false;
                $info['type'] = ($fixed) ? 'binary' : 'varbinary';
                break;
        }
    }

    protected function validateColumnSettings(array &$info)
    {
        // override this in each schema class
        $type = (isset($info['type'])) ? $info['type'] : null;
        switch (strtolower($type)) {
            // some types need massaging, some need other required properties
            case 'smallint':
            case 'int':
            case 'bigint':
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default) && is_numeric($default)) {
                    $info['default'] = intval($default);
                }
                break;

            case 'decimal':
            case 'numeric':
            case 'real':
            case 'float':
            case 'double':
                if (!isset($info['type_extras'])) {
                    $length =
                        (isset($info['length']))
                            ? $info['length']
                            : ((isset($info['precision'])) ? $info['precision']
                            : null);
                    if (!empty($length)) {
                        $scale =
                            (isset($info['decimals']))
                                ? $info['decimals']
                                : ((isset($info['scale'])) ? $info['scale']
                                : null);
                        $info['type_extras'] = (!empty($scale)) ? "($length,$scale)" : "($length)";
                    }
                }

                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default) && is_numeric($default)) {
                    $info['default'] = floatval($default);
                }
                break;

            case 'character':
            case 'graphic':
            case 'binary':
            case 'varchar':
            case 'vargraphic':
            case 'varbinary':
            case 'clob':
            case 'dbclob':
            case 'blob':
                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : 255);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                }
                break;

            case 'time':
            case 'timestamp':
            case 'datetime':
                $default = (isset($info['default'])) ? $info['default'] : null;
                if ('0000-00-00 00:00:00' == $default) {
                    // read back from MySQL has formatted zeros, can't send that back
                    $info['default'] = 0;
                }

                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                }
                break;
        }
    }

    /**
     * @param array $info
     *
     * @return string
     * @throws \Exception
     */
    protected function buildColumnDefinition(array $info)
    {
        $type = (isset($info['type'])) ? $info['type'] : null;
        $typeExtras = (isset($info['type_extras'])) ? $info['type_extras'] : null;

        $definition = $type . $typeExtras;

        $allowNull = (isset($info['allow_null'])) ? filter_var($info['allow_null'], FILTER_VALIDATE_BOOLEAN) : false;
        $definition .= ($allowNull) ? ' NULL' : ' NOT NULL';

        $default = (isset($info['default'])) ? $info['default'] : null;
        if (isset($default)) {
            $quoteDefault =
                (isset($info['quote_default'])) ? filter_var($info['quote_default'], FILTER_VALIDATE_BOOLEAN) : false;
            if ($quoteDefault) {
                $default = "'" . $default . "'";
            }

            if ('generated by default for each row on update as row change timestamp' === $default) {
                $definition .= ' ' . $default;
            } else {
                $definition .= ' DEFAULT ' . $default;
            }
        }

        $isUniqueKey = (isset($info['is_unique'])) ? filter_var($info['is_unique'], FILTER_VALIDATE_BOOLEAN) : false;
        $isPrimaryKey =
            (isset($info['is_primary_key'])) ? filter_var($info['is_primary_key'], FILTER_VALIDATE_BOOLEAN) : false;
        if ($isPrimaryKey && $isUniqueKey) {
            throw new \Exception('Unique and Primary designations not allowed simultaneously.');
        }
        if ($isUniqueKey) {
            $definition .= ' UNIQUE';
        } elseif ($isPrimaryKey) {
            $definition .= ' PRIMARY KEY';
        }

        $auto = (isset($info['auto_increment'])) ? filter_var($info['auto_increment'], FILTER_VALIDATE_BOOLEAN) : false;
        if ($auto) {
            $definition .= ' GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1)';
        }

        return $definition;
    }

    public function requiresCreateIndex($unique = false, $on_create_table = false)
    {
        return !($unique && $on_create_table);
    }

    /**
     * @inheritdoc
     */
    protected function loadTable(TableSchema $table)
    {
        if (!$this->findColumns($table)) {
            return null;
        }

        $this->findConstraints($table);

        return $table;
    }

    /**
     * Collects the table column metadata.
     *
     * @param TableSchema $table the table metadata
     *
     * @return boolean whether the table exists in the database
     */
    protected function findColumns($table)
    {
        $schema = (!empty($table->schemaName)) ? $table->schemaName : $this->getDefaultSchema();

        if ($this->isISeries()) {
            $sql = <<<MYSQL
SELECT column_name AS colname,
       ordinal_position AS colno,
       data_type AS typename,
       CAST(column_default AS VARCHAR(254)) AS default,
       is_nullable AS nulls,
       length AS length,
       numeric_scale AS scale,
       is_identity AS identity
FROM qsys2.syscolumns
WHERE table_name = :table AND table_schema = :schema
ORDER BY ordinal_position
MYSQL;
        } else {
            $sql = <<<MYSQL
SELECT colname,
       colno,
       typename,
       CAST(default AS VARCHAR(254)) AS default,
       nulls,
       length,
       scale,
       identity
FROM syscat.columns
WHERE syscat.columns.tabname = :table AND syscat.columns.tabschema = :schema
ORDER BY colno
MYSQL;
        }

        $columns = $this->connection->select($sql, [':table' => $table->tableName, ':schema' => $schema]);

        if (empty($columns)) {
            return false;
        }

        foreach ($columns as $column) {
            $c = $this->createColumn(array_change_key_case((array)$column, CASE_UPPER));
            $table->addColumn($c);
        }

        return true;
    }

    /**
     * Creates a table column.
     *
     * @param array $column column metadata
     *
     * @return ColumnSchema normalized column metadata
     */
    protected function createColumn($column)
    {
        $c = new ColumnSchema(['name' => $column['COLNAME']]);
        $c->rawName = $this->quoteColumnName($c->name);
        $c->allowNull = ($column['NULLS'] == 'Y');
        $c->autoIncrement = ($column['IDENTITY'] == 'Y');
        $c->dbType = $column['TYPENAME'];

        if (preg_match('/(varchar|character|clob|graphic|binary|blob)/i', $column['TYPENAME'])) {
            $c->size = $c->precision = $column['LENGTH'];
        } elseif (preg_match('/(decimal|double|real)/i', $column['TYPENAME'])) {
            $c->size = $c->precision = $column['LENGTH'];
            $c->scale = $column['SCALE'];
        }

        $c->fixedLength = $this->extractFixedLength($column['TYPENAME']);
        $c->supportsMultibyte = $this->extractMultiByteSupport($column['TYPENAME']);
        $this->extractType($c, $column['TYPENAME']);
        if (is_string($column['DEFAULT'])) {
            $column['DEFAULT'] = trim($column['DEFAULT'], '\'');
        }
        $default = ($column['DEFAULT'] == "NULL") ? null : $column['DEFAULT'];

        $this->extractDefault($c, $default);

        return $c;
    }

    /**
     * Collects the primary and foreign key column details for the given table.
     *
     * @param TableSchema $table the table metadata
     */
    protected function findConstraints($table)
    {
        $this->findPrimaryKey($table);

        if ($this->isISeries()) {
            $sql = <<<MYSQL
SELECT
  parent.table_schema AS referenced_table_schema,
  parent.table_name AS referenced_table_name,
  parent.column_name AS referenced_column_name,
  child.table_schema AS table_schema,
  child.table_name AS table_name,
  child.column_name AS column_name
FROM qsys2.syskeycst child
INNER JOIN qsys2.sysrefcst crossref
    ON child.constraint_schema = crossref.constraint_schema
   AND child.constraint_name = crossref.constraint_name
INNER JOIN qsys2.syskeycst parent
    ON crossref.unique_constraint_schema = parent.constraint_schema
   AND crossref.unique_constraint_name = parent.constraint_name
INNER JOIN qsys2.syscst coninfo
    ON child.constraint_name = coninfo.constraint_name
WHERE child.table_name = :table AND child.table_schema = :schema
  AND coninfo.constraint_type = 'FOREIGN KEY'
MYSQL;
        } else {
            $sql = <<<MYSQL
SELECT fk.tabschema AS table_schema, fk.tabname AS table_name, fk.colname AS column_name,
	pk.tabschema AS referenced_table_schema, pk.tabname AS referenced_table_name, pk.colname AS referenced_column_name
FROM syscat.references
INNER JOIN syscat.keycoluse AS fk ON fk.constname = syscat.references.constname
INNER JOIN syscat.keycoluse AS pk ON pk.constname = syscat.references.refkeyname AND pk.colseq = fk.colseq
MYSQL;
        }

        $constraints = $this->connection->select($sql);

        $this->buildTableRelations($table, $constraints);
    }

    /**
     * Gets the primary key column(s) details for the given table.
     *
     * @param TableSchema $table table
     *
     * @return mixed primary keys (null if no pk, string if only 1 column pk, or array if composite pk)
     */
    protected function findPrimaryKey($table)
    {
        $schema = (!empty($table->schemaName)) ? $table->schemaName : $this->getDefaultSchema();

        if ($this->isISeries()) {
            $sql = <<<MYSQL
SELECT column_name As colnames
FROM qsys2.syscst 
INNER JOIN qsys2.syskeycst
  ON qsys2.syscst.constraint_name = qsys2.syskeycst.constraint_name
 AND qsys2.syscst.table_schema = qsys2.syskeycst.table_schema 
 AND qsys2.syscst.table_name = qsys2.syskeycst.table_name
WHERE qsys2.syscst.constraint_type = 'PRIMARY KEY'
  AND qsys2.syscst.table_name = :table AND qsys2.syscst.table_schema = :schema
MYSQL;
        } else {
            $sql = <<<MYSQL
SELECT colnames AS colnames
FROM syscat.indexes
WHERE uniquerule = 'P'
  AND tabname = :table AND tabschema = :schema
MYSQL;
        }

        $indexes = $this->connection->select($sql, [':table' => $table->tableName, ':schema' => $schema]);
        foreach ($indexes as $index) {
            $index = array_change_key_case((array)$index, CASE_UPPER);
            $columns = explode("+", ltrim($index['COLNAMES'], '+'));
            foreach ($columns as $colname) {
                $column = $table->getColumn($colname);
                if (isset($column)) {
                    $column->isPrimaryKey = true;
                    if ((DbSimpleTypes::TYPE_INTEGER === $column->type) && $column->autoIncrement) {
                        $column->type = DbSimpleTypes::TYPE_ID;
                    }
                    if ($table->primaryKey === null) {
                        $table->primaryKey = $colname;
                    } elseif (is_string($table->primaryKey)) {
                        $table->primaryKey = [$table->primaryKey, $colname];
                    } else {
                        $table->primaryKey[] = $colname;
                    }
                    // update the column in the table
                    $table->addColumn($column);
                }
            }
        }

        /* @var $c ColumnSchema */
        foreach ($table->getColumns() as $c) {
            if ($c->autoIncrement && $c->isPrimaryKey) {
                $table->sequenceName = $c->rawName;
                break;
            }
        }
    }

    protected function findSchemaNames()
    {
        if ($this->isISeries()) {
//            $sql = <<<MYSQL
//SELECT DISTINCT TABLE_SCHEMA as SCHEMANAME FROM QSYS2.SYSTABLES WHERE SYSTEM_TABLE = 'N' ORDER BY SCHEMANAME;
//MYSQL;
            $sql = <<<MYSQL
SELECT SCHEMA_NAME as SCHEMANAME FROM QSYS2.SYSSCHEMAS ORDER BY SCHEMANAME;
MYSQL;
        } else {
            $sql = <<<MYSQL
SELECT SCHEMANAME FROM SYSCAT.SCHEMATA WHERE DEFINERTYPE != 'S' ORDER BY SCHEMANAME;
MYSQL;
        }

        $rows = array_map('trim', $this->selectColumn($sql));

        $defaultSchema = $this->getDefaultSchema();
        if (!empty($defaultSchema) && (false === array_search($defaultSchema, $rows))) {
            $rows[] = $defaultSchema;
        }

        return $rows;
    }

    /**
     * Returns all table names in the database.
     *
     * @param string $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     *                       If not empty, the returned table names will be prefixed with the schema name.
     * @param bool   $include_views
     *
     * @return array all table names in the database.
     */
    protected function findTableNames($schema = '', $include_views = true)
    {
        if ($include_views) {
            $condition = "('T','V')";
        } else {
            $condition = "('T')";
        }

        if ($this->isISeries()) {
            $sql = <<<MYSQL
SELECT TABLE_SCHEMA as TABSCHEMA, TABLE_NAME as TABNAME, TABLE_TYPE AS TYPE
FROM QSYS2.SYSTABLES
WHERE TABLE_TYPE IN $condition AND SYSTEM_TABLE = 'N'
MYSQL;
            if ($schema !== '') {
                $sql .= <<<MYSQL
  AND TABLE_SCHEMA = :schema
MYSQL;
            }
        } else {
            $sql = <<<MYSQL
SELECT TABSCHEMA, TABNAME, TYPE
FROM SYSCAT.TABLES
WHERE TYPE IN $condition AND OWNERTYPE != 'S'
MYSQL;
            if (!empty($schema)) {
                $sql .= <<<MYSQL
  AND TABSCHEMA=:schema
MYSQL;
            }
        }
        $sql .= <<<MYSQL
  ORDER BY TABNAME;
MYSQL;

        $params = (!empty($schema)) ? [':schema' => $schema] : [];
        $rows = $this->connection->select($sql, $params);

        $defaultSchema = $this->getDefaultSchema();
        $addSchema = (!empty($schema) && ($defaultSchema !== $schema));

        $names = [];
        foreach ($rows as $row) {
            $row = array_change_key_case((array)$row, CASE_UPPER);
            $schemaName = trim(isset($row['TABSCHEMA']) ? $row['TABSCHEMA'] : '');
            $tableName = trim(isset($row['TABNAME']) ? $row['TABNAME'] : '');
            $isView = (0 === strcasecmp('V', $row['TYPE']));
            if ($addSchema) {
                $name = $schemaName . '.' . $tableName;
                $rawName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($tableName);;
            } else {
                $name = $tableName;
                $rawName = $this->quoteTableName($tableName);
            }
            $settings = compact('schemaName', 'tableName', 'name', 'rawName', 'isView');
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }

    /**
     * Resets the sequence value of a table's primary key.
     * The sequence will be reset such that the primary key of the next new row inserted
     * will have the specified value or 1.
     *
     * @param TableSchema $table    the table schema whose primary key sequence will be reset
     * @param mixed       $value    the value for the primary key of the next new row inserted. If this is not set,
     *                              the next new row's primary key will have a value 1.
     */
    public function resetSequence($table, $value = null)
    {
        if ($table->sequenceName !== null &&
            is_string($table->primaryKey) &&
            $table->getColumn($table->primaryKey)->autoIncrement
        ) {
            if ($value === null) {
                $value = $this->selectValue("SELECT MAX({$table->primaryKey}) FROM {$table->rawName}") + 1;
            } else {
                $value = (int)$value;
            }

            $this->connection
                ->statement("ALTER TABLE {$table->rawName} ALTER COLUMN {$table->primaryKey} RESTART WITH $value");
        }
    }

    /**
     * Enables or disables integrity check.
     *
     * @param boolean $check  whether to turn on or off the integrity check.
     * @param string  $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     *
     * @since 1.1
     */
    public function checkIntegrity($check = true, $schema = '')
    {
        $enable = $check ? 'CHECKED' : 'UNCHECKED';
        $tableNames = $this->getTableNames($schema);
        $db = $this->connection;
        foreach ($tableNames as $tableInfo) {
            $tableName = $tableInfo['name'];
            $db->statement("SET INTEGRITY FOR $tableName ALL IMMEDIATE $enable");
        }
    }

    /**
     * {@InheritDoc}
     */
    public function addForeignKey($name, $table, $columns, $refTable, $refColumns, $delete = null, $update = null)
    {
        switch (strtoupper($update)) {
            case 'CASCADE':
            case 'SET NULL':
                $update = null; // not supported on update, only NO ACTION and RESTRICT
                break;
        }

        return parent::addForeignKey($name, $table, $columns, $refTable, $refColumns, $delete, null);
    }

    /**
     * Builds a SQL statement for truncating a DB table.
     *
     * @param string $table the table to be truncated. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for truncating a DB table.
     * @since 1.1.6
     */
    public function truncateTable($table)
    {
        return "TRUNCATE TABLE " . $this->quoteTableName($table) . " IMMEDIATE ";
    }

    /**
     * Builds a SQL statement for changing the definition of a column.
     *
     * @param string $table      the table whose column is to be changed. The table name will be properly quoted by the
     *                           method.
     * @param string $column     the name of the column to be changed. The name will be properly quoted by the method.
     * @param string $definition the new column type. The {@link getColumnType} method will be invoked to convert
     *                           abstract column type (if any) into the physical one. Anything that is not recognized
     *                           as abstract type will be kept in the generated SQL. For example, 'string' will be
     *                           turned into 'varchar(255)', while 'string not null' will become 'varchar(255) not
     *                           null'.
     *
     * @return string the SQL statement for changing the definition of a column.
     */
    public function alterColumn($table, $column, $definition)
    {
        $tableSchema = $this->getTable($table);
        $columnSchema = $tableSchema->getColumn(rtrim($column));

        $allowNullNewType = !preg_match("/not +null/i", $definition);

        $definition = preg_replace("/ +(not)? *null/i", "", $definition);

        $sql = <<<MYSQL
ALTER TABLE {$this->quoteTableName($table)} 
ALTER COLUMN {$this->quoteColumnName($column)} SET DATA TYPE {$this->getColumnType($definition)}
MYSQL;

        if ($columnSchema->allowNull != $allowNullNewType) {
            if ($allowNullNewType) {
                $sql .= ' ALTER COLUMN ' . $this->quoteColumnName($column) . 'DROP NOT NULL';
            } else {
                $sql .= ' ALTER COLUMN ' . $this->quoteColumnName($column) . 'SET NOT NULL';
            }
        }

        return $sql;
    }

    /**
     * @return string default schema.
     */
    public function findDefaultSchema()
    {
        $sql = <<<MYSQL
VALUES CURRENT_SCHEMA
MYSQL;

        return $this->selectValue($sql);
    }

    protected function findRoutineNames($type, $schema = '')
    {
        if ($this->isISeries()) {
            $bindings = [':type' => $type];
            $where = "FUNCTION_ORIGIN != 'S' AND ROUTINE_TYPE = :type";
            if (!empty($schema)) {
                $where .= ' AND ROUTINE_SCHEMA = :schema';
                $bindings[':schema'] = $schema;
            }

            $sql = <<<MYSQL
SELECT ROUTINE_NAME AS ROUTINENAME, FUNCTION_TYPE AS FUNCTIONTYPE FROM QSYS2.SYSROUTINES WHERE {$where}
MYSQL;
        } else {
            $where = "OWNERTYPE != 'S' AND ROUTINETYPE = :type";
            if (!empty($schema)) {
                $where .= ' AND ROUTINESCHEMA = :schema';
                $bindings[':schema'] = $schema;
            }

            $sql = <<<MYSQL
SELECT ROUTINENAME, RETURN_TYPENAME, FUNCTIONTYPE FROM SYSCAT.ROUTINES WHERE {$where}
MYSQL;
        }
        $rows = $this->connection->select($sql, $bindings);

        $defaultSchema = $this->getDefaultSchema();
        $addSchema = (!empty($schema) && ($defaultSchema !== $schema));

        $names = [];
        foreach ($rows as $row) {
            $row = array_change_key_case((array)$row, CASE_UPPER);
            $name = array_get($row, 'ROUTINENAME');
            $schemaName = $schema;
            if ($addSchema) {
                $publicName = $schemaName . '.' . $name;
                $rawName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($name);;
            } else {
                $publicName = $name;
                $rawName = $this->quoteTableName($name);
            }
            if (!empty($returnType = array_get($row, 'RETURN_TYPENAME'))) {
                $returnType = static::extractSimpleType($returnType);
            } else {
                switch ($functionType = array_get($row, 'FUNCTIONTYPE')) {
                    case 'R': // row
                        $returnType = 'row';
                        break;
                    case 'T': // table
                        $returnType = 'table';
                        break;
                    case 'C': // column or aggregate
                        $returnType = 'column';
                        break;
                    case 'S': // scalar, return type should be set
                        break;
                    default: // procedure
                        break;
                }
            }
            $settings = compact('schemaName', 'name', 'publicName', 'rawName', 'returnType');
            $names[strtolower($publicName)] =
                ('PROCEDURE' === $type) ? new ProcedureSchema($settings) : new FunctionSchema($settings);
        }

        return $names;
    }

    protected function loadParameters(&$holder)
    {
        if ($this->isISeries()) {
            $sql = <<<MYSQL
SELECT ORDINAL_POSITION, PARAMETER_MODE, ROW_TYPE, PARAMETER_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH, DEFAULT
FROM QSYS2.SYSPARMS
WHERE SPECIFIC_NAME = '{$holder->name}' AND SPECIFIC_SCHEMA = '{$holder->schemaName}'
MYSQL;

            $rows = $this->connection->select($sql);
            foreach ($rows as $row) {
                $row = array_change_key_case((array)$row, CASE_UPPER);
                $paramName = array_get($row, 'PARAMETER_NAME');
                $dbType = array_get($row, 'DATA_TYPE');
                $simpleType = static::extractSimpleType($dbType);
                $pos = intval(array_get($row, 'ORDINAL_POSITION'));
                $length = (isset($row['CHARACTER_MAXIMUM_LENGTH']) ? intval(array_get($row, 'CHARACTER_MAXIMUM_LENGTH')) : null);
                $precision = (isset($row['NUMERIC_PRECISION']) ? intval(array_get($row, 'NUMERIC_PRECISION')) : null);
                $scale = (isset($row['NUMERIC_SCALE']) ? intval(array_get($row, 'NUMERIC_SCALE')) : null);
                switch (strtoupper(array_get($row, 'ROW_TYPE', ''))) {
                    case 'P':
                        $paramType = array_get($row, 'PARAMETER_MODE');
                        $holder->addParameter(new ParameterSchema(
                            [
                                'name'          => $paramName,
                                'position'      => $pos,
                                'param_type'    => $paramType,
                                'type'          => $simpleType,
                                'db_type'       => $dbType,
                                'length'        => $length,
                                'precision'     => $precision,
                                'scale'         => $scale,
                                'default_value' => array_get($row, 'DEFAULT'),
                            ]
                        ));
                        break;
                    case 'R':
                    case 'C':
                        $holder->returnSchema[] = [
                            'name'      => $paramName,
                            'position'  => $pos,
                            'type'      => $simpleType,
                            'db_type'   => $dbType,
                            'length'    => $length,
                            'precision' => $precision,
                            'scale'     => $scale,
                        ];
                        break;
                    default:
                        break;
                }
            }
        } else {
            $sql = <<<MYSQL
SELECT ORDINAL, ROWTYPE, PARMNAME, TYPENAME, LENGTH, SCALE, DEFAULT
FROM SYSCAT.ROUTINEPARMS
WHERE ROUTINENAME = '{$holder->name}' AND ROUTINESCHEMA = '{$holder->schemaName}'
MYSQL;
        }

        $rows = $this->connection->select($sql);
        foreach ($rows as $row) {
            $row = array_change_key_case((array)$row, CASE_UPPER);
            $paramName = array_get($row, 'PARMNAME');
            $dbType = array_get($row, 'TYPENAME');
            $simpleType = static::extractSimpleType($dbType);
            $pos = intval(array_get($row, 'ORDINAL'));
            $length = (isset($row['LENGTH']) ? intval(array_get($row, 'LENGTH')) : null);
            $scale = (isset($row['SCALE']) ? intval(array_get($row, 'SCALE')) : null);
            switch (strtoupper(array_get($row, 'ROWTYPE', ''))) {
                case 'P':
                    $paramType = 'IN';
                    break;
                case 'B':
                    $paramType = 'INOUT';
                    break;
                case 'O':
                    $paramType = 'OUT';
                    break;
                case 'R':
                case 'C':
                    $holder->returnSchema[] = [
                        'name'      => $paramName,
                        'position'  => $pos,
                        'type'      => $simpleType,
                        'db_type'   => $dbType,
                        'length'    => $length,
                        'precision' => $length,
                        'scale'     => $scale,
                    ];
                    continue 2;
                    break;
                default:
                    continue 2;
                    break;
            }
            if (0 === $pos) {
                if (empty($holder->returnType)) {
                    $holder->returnType = $simpleType;
                }
            } else {
                $holder->addParameter(new ParameterSchema(
                    [
                        'name'          => $paramName,
                        'position'      => $pos,
                        'param_type'    => $paramType,
                        'type'          => $simpleType,
                        'db_type'       => $dbType,
                        'length'        => $length,
                        'precision'     => $length,
                        'scale'         => $scale,
                        'default_value' => array_get($row, 'DEFAULT'),
                    ]
                ));
            }
        }
    }

    public function getTimestampForSet()
    {
        return $this->connection->raw('(CURRENT TIMESTAMP)');
    }

    public function parseValueForSet($value, $field_info)
    {
        switch ($field_info->type) {
            case DbSimpleTypes::TYPE_BOOLEAN:
                $value = (filter_var($value, FILTER_VALIDATE_BOOLEAN) ? 1 : 0);
                break;
            case DbSimpleTypes::TYPE_INTEGER:
                $value = intval($value);
                break;
            case DbSimpleTypes::TYPE_DECIMAL:
                $value = number_format(floatval($value), $field_info->scale, '.', '');
                break;
            case DbSimpleTypes::TYPE_DOUBLE:
            case DbSimpleTypes::TYPE_FLOAT:
                $value = floatval($value);
                break;
        }

        return $value;
    }

    /**
     * @inheritdoc
     */
    protected function getFunctionStatement(RoutineSchema $routine, array $param_schemas, array &$values)
    {
        switch ($routine->returnType) {
            case 'row':
            case 'table':
                $paramStr = $this->getRoutineParamString($param_schemas, $values);

                /** @noinspection SqlDialectInspection */
                return "SELECT * from TABLE({$routine->rawName}($paramStr))";
                break;
            default:
                return parent::getFunctionStatement($routine, $param_schemas, $values) . ' FROM SYSIBM.SYSDUMMY1';
                break;
        }
    }
}
