<?php

namespace App\Extend\DB;

use Exception;
use PDO;
use Swoole\Database\PDOConfig;
use Swoole\Database\PDOPool;
use Swoole\Database\PDOProxy;
use Throwable;

class Connection
{
    private static array $instance;
    protected PDOPool $pools;

    protected array $config
        = [
            'host' => 'localhost',
            'port' => 3306,
            'database' => 'test',
            'username' => 'root',
            'password' => 'root',
            'charset' => 'utf8mb4',
            'unixSocket' => null,
            'timeout' => 45,
            'options' => [
                19 => 2,
                20 => false,
                17 => false,
            ],
            'size' => 32,
        ];

    /**
     * @throws Exception
     */
    private function __construct(array $config)
    {
        if (empty($this->pools)) {
            $this->config = array_replace_recursive($this->config, $config);
            try {
                $this->pools = new PDOPool(
                    (new PDOConfig())
                        ->withHost($this->config['host'])
                        ->withPort($this->config['port'])
                        ->withUnixSocket($this->config['unixSocket'])
                        ->withDbName($this->config['database'])
                        ->withCharset($this->config['charset'])
                        ->withUsername($this->config['username'])
                        ->withPassword($this->config['password'])
                        ->withOptions($this->config['options']),
                    $this->config['size']
                );
            } catch (Throwable $th) {
                throw new Exception($th->getMessage());
            }
        }
    }

    /**
     * @throws Exception
     */
    public static function getInstance($config = null, $poolName = 'default')
    {
        try {
            if (empty(self::$instance[$poolName])) {
                self::$instance[$poolName] = new static($config);
            }
            return self::$instance[$poolName];
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function getConnection(): PDO|PDOProxy
    {
        try {
            return $this->pools->get();
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function close($connection = null): void
    {
        try {
            $this->pools->put($connection);
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }
}