<?php

namespace App\Extend\Redis;

use EasySwoole\EasySwoole\Config;
use EasySwoole\Utility\SnowFlake;
use Exception;
use Redis as RedisBase;
use Throwable;

/**
 * @method get(string $key)
 * @method set(string $key, string $value, int[] $array)
 * @method del(string $key)
 */
class Redis
{
    protected mixed $pool;
    protected RedisBase $connection;
    protected bool $multiOnGoing = false;

    public function __construct($configName='default')
    {
        $config = Config::getInstance()->getConf('Redis.' . $configName);
        $this->pool = Connection::getInstance($config, $configName);
    }

    /**
     * @throws Exception
     */
    public function __call($name, $arguments)
    {
        try {
            if (!$this->multiOnGoing) {
                $this->connection = $this->pool->getConnection();
            }
            $data = $this->connection->{$name}(...$arguments);
            if ($this->multiOnGoing) {
                return $this;
            }

            return $data;
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        } finally {
            $this->pool->close($this->connection ?? null);
        }
    }

    public function getValue(string $key): ?string
    {
        return $this->get($key);
    }

    public function setValue(string $key, string $value, int $options): bool
    {
        return $this->set($key, $value, ['ex' => $options]);
    }

    public function delValue(string $key): bool
    {
        return $this->del($key);
    }

    /**
     * @throws Exception
     */
    public function brPop($keys, $timeout): array
    {
        try {
            $this->connection = $this->pool->getConnection();
            $data = $this->connection->brPop($keys, $timeout);
            $this->pool->close($this->connection);

            return $data;
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function blPop($keys, $timeout): array
    {
        try {
            $this->connection = $this->pool->getConnection();

            return $this->connection->blPop($keys, $timeout);
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        } finally {
            $this->pool->close($this->connection);
        }
    }

    /**
     * @throws Exception
     */
    public function subscribe($channels, $callback): false|array|RedisBase
    {
        try {
            $this->connection = $this->pool->getConnection();
            $this->connection->setOption(RedisBase::OPT_READ_TIMEOUT, '-1');
            $data = $this->connection->subscribe($channels, $callback);
            $this->connection->setOption(RedisBase::OPT_READ_TIMEOUT, (string)$this->pool->getConfig()['time_out']);
            $this->pool->close($this->connection);
            return $data;
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function fill(): void
    {
        try {
            $this->pool->fill();
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function multi($mode = RedisBase::MULTI): Redis
    {
        try {
            if (!$this->multiOnGoing) {
                $this->connection = $this->pool->getConnection();
                $this->connection->multi($mode);
                $this->multiOnGoing = true;
            }
            return $this;
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function exec(): false|array|RedisBase|null
    {
        try {
            if (!$this->multiOnGoing) {
                return null;
            }
            $result = $this->connection->exec();
            $this->multiOnGoing = false;
            $this->pool->close($this->connection);
            return $result;
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function discard(): void
    {
        try {
            if (!$this->multiOnGoing) {
                return;
            }
            $this->pool->close($this->connection);
            $this->multiOnGoing = false;
            $this->connection->discard();
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function lockFunction($key, callable $callback, $ttl = 60)
    {
        try {
            $random = $this->lock($key, $ttl);
            if ($random === null) {
                return false;
            }
            $result = call_user_func($callback);
            $this->unlock($key, $random);

            return $result;
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function lock($key, $ttl = 60): ?string
    {
        try {
            $this->connection = $this->pool->getConnection();
            $randNum = (new Snowflake())->make();
            $result = $this->connection->set($key, $randNum, ['NX', 'PX' => $ttl * 1000]);
            if ($result) {
                return $randNum;
            }
            return null;
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        } finally {
            $this->pool->close($this->connection);
        }
    }

    /**
     * @throws Exception
     */
    public function unlock($key, $randNum)
    {
        try {
            $lua = <<<LUA
								if redis.call('get',KEYS[1]) == ARGV[1] then 
								   return redis.call('del',KEYS[1]) 
								else
								   return 0 
								end
								LUA;
            $this->connection = $this->pool->getConnection();
            return $this->connection->eval($lua, [$key, $randNum], 1);
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        } finally {
            $this->pool->close($this->connection);
        }
    }

    /**
     * @throws Exception
     */
    public function rateLimit($key, $limitNum = 200, $ttl = 5)
    {
        try {
            $lua = <<<SCRIPT
									            redis.call('zAdd',KEYS[1],tonumber(ARGV[2]),ARGV[3])
									            redis.call('zRemRangeByScore',KEYS[1],0,tonumber(ARGV[2])-tonumber(ARGV[1]))
									            redis.call('expire',KEYS[1],tonumber(ARGV[1]))
									            local num = redis.call('zCount',KEYS[1],0,tonumber(ARGV[2]))
									            if num > tonumber(ARGV[4]) then
									                return 1
									            else
									                return 0
									            end
									SCRIPT;
            $this->connection = $this->pool->getConnection();
            $score = time();
            $nonce = (new Snowflake())->make();
            return $this->connection->eval($lua, [$key, $ttl, $score, $nonce, $limitNum], 1);
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        } finally {
            $this->pool->close($this->connection);
        }
    }
}
