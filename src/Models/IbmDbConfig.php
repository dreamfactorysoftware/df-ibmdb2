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

    public static function getDefaultPort()
    {
        return 56789;
    }
}