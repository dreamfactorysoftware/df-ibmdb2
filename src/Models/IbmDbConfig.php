<?php
namespace DreamFactory\Core\IbmDb2\Models;

use DreamFactory\Core\SqlDb\Models\SqlDbConfig;

/**
 * IbmDbConfig
 *
 */
class IbmDbConfig extends SqlDbConfig
{
    public static function getDriverName()
    {
        return 'ibm';
    }

    public static function getDefaultDsn()
    {
        // http://php.net/manual/en/ref.pdo-ibm.connection.php
        return 'ibm:DRIVER={IBM DB2 ODBC DRIVER};DATABASE=db;HOSTNAME=localhost;PORT=56789;PROTOCOL=TCPIP;';
    }

    public static function getDefaultPort()
    {
        return 56789;
    }
}