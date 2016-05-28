<?php

namespace DreamFactory\Core\IbmDb2\Services;

use DreamFactory\Core\SqlDb\Services\SqlDb;

/**
 * Class IbmDb2
 *
 * @package DreamFactory\Core\SqlDb\Services
 */
class IbmDb2 extends SqlDb
{
    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'ibm';
        parent::adaptConfig($config);
    }
}