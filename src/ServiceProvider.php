<?php
namespace DreamFactory\Core\IbmDb2;

use DreamFactory\Core\Components\DbSchemaExtensions;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\IbmDb2\Database\Connectors\IbmConnector;
use DreamFactory\Core\IbmDb2\Database\IbmConnection;
use DreamFactory\Core\IbmDb2\Database\Schema\IbmSchema;
use DreamFactory\Core\IbmDb2\Models\IbmDbConfig;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use DreamFactory\Core\IbmDb2\Services\IbmDb2;
use Illuminate\Database\DatabaseManager;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {
        // Add our database drivers
        $this->app->resolving('db', function (DatabaseManager $db) {
            $db->extend('ibm', function ($config) {
                $connector = new IbmConnector();
                $connection = $connector->connect($config);

                return new IbmConnection($connection, $config["database"], $config["prefix"], $config);
            });
        });

        // Add our service types
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'            => 'ibmdb2',
                    'label'           => 'IBM DB2',
                    'description'     => 'Database service supporting IBM DB2 SQL connections.',
                    'group'           => ServiceTypeGroups::DATABASE,
                    'config_handler'  => IbmDbConfig::class,
                    'factory'         => function ($config) {
                        return new IbmDb2($config);
                    },
                ])
            );
        });

        // Add our database extensions
        $this->app->resolving('db.schema', function (DbSchemaExtensions $db) {
            $db->extend('ibm', function ($connection) {
                return new IbmSchema($connection);
            });
        });
    }
}
