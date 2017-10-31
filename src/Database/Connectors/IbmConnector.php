<?php

namespace DreamFactory\Core\IbmDb2\Database\Connectors;

use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;

class IbmConnector extends Connector implements ConnectorInterface
{
    /**
     * Establish a database connection.
     *
     * @param  array $config
     * @return \PDO
     * @throws \Exception
     */
    public function connect(array $config)
    {
        $dsn = $this->getDsn($config);
        $options = $this->getOptions($config);

        return $this->createConnection($dsn, $config, $options);
    }

    /**
     * Create a DSN string from a configuration.
     *
     * @param  array $config
     * @return string
     */
    protected function getDsn(array $config)
    {
        extract($config, EXTR_SKIP);

        $dsn = "ibm:";

        if (empty($driverName)) {
            $driverName = "{IBM DB2 ODBC DRIVER}";
        }
        $dsn .= "DRIVER={$driverName};";
        if (!empty($host)) {
            $dsn .= "HOSTNAME={$host};";
        }
        if (!empty($port)) {
            $dsn .= "PORT={$port};";
        }
        if (!empty($protocol)) {
            $dsn .= "PROTOCOL={$protocol};";
        }
        if (!empty($database)) {
            $dsn .= "DATABASE={$database};";
        }


        return $dsn;
    }
}
