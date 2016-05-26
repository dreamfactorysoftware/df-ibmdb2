<?php

namespace DreamFactory\Core\IbmDb2\Database;

use DreamFactory\Core\IbmDb2\Database\Query\Processors\IbmProcessor;
use DreamFactory\Core\IbmDb2\Database\Query\Grammars\IbmGrammar as QueryGrammar;
use DreamFactory\Core\IbmDb2\Database\Schema\Grammars\IbmGrammar as SchemaGrammar;
use Illuminate\Database\Connection;
use PDO;

class IbmConnection extends Connection
{
    /**
     * The name of the default schema.
     *
     * @var string
     */
    protected $defaultSchema;

    public function __construct(PDO $pdo, $database = '', $tablePrefix = '', array $config = [])
    {
        parent::__construct($pdo, $database, $tablePrefix, $config);
        if (isset($config['schema'])) {
            $this->currentSchema = $this->defaultSchema = strtoupper($config['schema']);
        }
    }

    /**
     * Get the name of the default schema.
     *
     * @return string
     */
    public function getDefaultSchema()
    {
        return $this->defaultSchema;
    }

    /**
     * Reset to default the current schema.
     *
     * @return string
     */
    public function resetCurrentSchema()
    {
        $this->setCurrentSchema($this->getDefaultSchema());
    }

    /**
     * Set the name of the current schema.
     *
     * @return string
     */
    public function setCurrentSchema($schema)
    {
        //$this->currentSchema = $schema;
        $this->statement('SET SCHEMA ?', [strtoupper($schema)]);
    }

    /**
     * @return QueryGrammar
     */
    protected function getDefaultQueryGrammar()
    {
        return $this->withTablePrefix(new QueryGrammar);
    }

    /**
     * Default grammar for specified Schema
     * @return SchemaGrammar
     */
    protected function getDefaultSchemaGrammar()
    {
        return $this->withTablePrefix(new SchemaGrammar);
    }

    /**
    * Get the default post processor instance.
    *
    * @return IbmProcessor
    */
    protected function getDefaultPostProcessor()
    {
        return new IbmProcessor;
    }
}
