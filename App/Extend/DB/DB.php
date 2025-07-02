<?php

namespace App\Extend\DB;

use EasySwoole\EasySwoole\Config;
use EasySwoole\EasySwoole\Logger;
use Exception;
use InvalidArgumentException;
use PDO;
use PDOException;
use PDOStatement;
use Swoole\Coroutine;
use Swoole\Database\PDOProxy;
use Swoole\Database\PDOStatementProxy;
use Throwable;

class DB
{
    protected PDOProxy $pdo;
    protected Connection $pool;
    public string $type;
    public bool $inTransaction = false;
    protected string $prefix;
    protected ?PDOStatementProxy $statement = null;
    protected string $dsn;
    protected array $logs = [];
    protected bool $logging = false;
    protected bool $debugMode = false;
    protected int $guid = 0;
    public string $returnId = '';
    public string|null $error = null;

    public array|null $errorInfo = null;


    /**
     * @throws Exception
     */
    public function __construct(string $configName = 'default')
    {
        $config = Config::getInstance()->getConf('MYSQL.' . $configName);
        $this->pool = Connection::getInstance($config, $configName);
        $this->prefix = $config['prefix'];
        $this->type = 'mysql';
    }

    protected function mapKey(): string
    {
        return ':MeD' . $this->guid++ . '_mK';
    }

    public function query(string $statement, array $map = []): ?PDOStatement
    {
        $raw = $this->raw($statement, $map);
        $statement = $this->buildRaw($raw, $map);

        return $this->exec($statement, $map);
    }

    /**
     * @throws Exception
     */
    public function exec($query, $map = [])
    {
        try {
            $this->realGetConn();
            $this->statement = null;
            if ($this->debugMode) {
                echo $this->generate($query, $map);
                $this->debugMode = false;
                $this->release($this->pdo);

                return false;
            }
            if ($this->logging) {
                $this->logs[] = [$query, $map];
            } else {
                $this->logs = [[$query, $map]];
            }
            $statement = $this->pdo->prepare($query);
            if (!$statement) {
                $this->errorInfo = $this->pdo->errorInfo();
                $this->statement = null;
                $this->release($this->pdo);

                return false;
            }
            $this->statement = $statement;
            foreach ($map as $key => $value) {
                $statement->bindValue($key, $value[0], $value[1]);
            }
            $execute = $statement->execute();
            $this->errorInfo = $statement->errorInfo();
            if (!$execute) {
                $this->statement = null;
            }
            Logger::getInstance()->info($this->last(), 'Sql');
            $lastId = $this->pdo->lastInsertId();
            $this->release($this->pdo);
            if ($lastId != '0' && $lastId != '') {
                return $lastId;
            }
            return $statement;
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    protected function generate(string $statement, array $map): string
    {
        $identifier = [
            'mysql' => '`$1`',
            'mssql' => '[$1]'
        ];

        $statement = preg_replace(
            '/(?!\'\S+\s?)"([\p{L}_][\p{L}\p{N}@$#\-_]*)"(?!\s?\S+\')/u',
            $identifier[$this->type] ?? '"$1"',
            $statement
        );
        foreach ($map as $key => $value) {
            if ($value[1] === PDO::PARAM_STR) {
                $replace = $this->quote($value[0]);
            } elseif ($value[1] === PDO::PARAM_NULL) {
                $replace = 'NULL';
            } elseif ($value[1] === PDO::PARAM_LOB) {
                $replace = '{LOB_DATA}';
            } else {
                $replace = $value[0] . '';
            }
            $statement = str_replace($key, $replace, $statement);
        }
        return $statement;
    }

    public static function raw(string $string, array $map = []): Raw
    {
        $raw = new Raw();
        $raw->map = $map;
        $raw->value = $string;
        return $raw;
    }

    protected function isRaw($object): bool
    {
        return $object instanceof Raw;
    }

    protected function buildRaw($raw, array &$map): ?string
    {
        if (!$this->isRaw($raw)) {
            return null;
        }
        $query = preg_replace_callback(
            '/(([`\']).*?)?((FROM|TABLE|INTO|UPDATE|JOIN|TABLE IF EXISTS)\s*)?<(([\p{L}_][\p{L}\p{N}@$#\-_]*)(\.[\p{L}_][\p{L}\p{N}@$#\-_]*)?)>([^,]*?\2)?/u',
            function ($matches) {
                if (!empty($matches[2]) && isset($matches[8])) {
                    return $matches[0];
                }
                if (!empty($matches[4])) {
                    return $matches[1] . $matches[4] . ' ' . $this->tableQuote($matches[5]);
                }
                return $matches[1] . $this->columnQuote($matches[5]);
            },
            $raw->value
        );
        $rawMap = $raw->map;
        if (!empty($rawMap)) {
            foreach ($rawMap as $key => $value) {
                $map[$key] = $this->typeMap($value, gettype($value));
            }
        }
        return $query;
    }

    public function quote(string $string): string
    {
        if ($this->type === 'mysql') {
            return "'" . preg_replace(['/([\'"])/', '/(\\\\\")/'], ["\\\\\${1}", '\\\${1}'], $string) . "'";
        }

        return "'" . preg_replace('/\'/', '\'\'', $string) . "'";
    }

    /**
     * @throws Exception
     */
    public function tableQuote(string $table): string
    {
        if (preg_match('/^[\p{L}_][\p{L}\p{N}@$#\-_]*$/u', $table)) {
            return '"' . $this->prefix . $table . '"';
        }

        throw new Exception("表名错误: {$table}.");
    }

    /**
     * @throws Exception
     */
    public function columnQuote(string $column): string
    {
        if (preg_match('/^[\p{L}_][\p{L}\p{N}@$#\-_]*(\.?[\p{L}_][\p{L}\p{N}@$#\-_]*)?$/u', $column)) {
            return str_contains($column, '.') ? '"' . $this->prefix . str_replace('.', '"."', $column) . '"' : '"' . $column . '"';
        }
        throw new Exception("列名错误: {$column}.");
    }

    protected function typeMap($value, string $type): array
    {
        $map = [
            'NULL' => PDO::PARAM_NULL,
            'integer' => PDO::PARAM_INT,
            'double' => PDO::PARAM_STR,
            'boolean' => PDO::PARAM_BOOL,
            'string' => PDO::PARAM_STR,
            'object' => PDO::PARAM_STR,
            'resource' => PDO::PARAM_LOB
        ];
        if ($type === 'boolean') {
            $value = ($value ? '1' : '0');
        } elseif ($type === 'NULL') {
            $value = null;
        }
        return [$value, $map[$type]];
    }

    /**
     * @throws Exception
     */
    protected function columnPush(&$columns, array &$map, bool $root, bool $isJoin = false): string
    {
        if ($columns === '*') {
            return $columns;
        }
        $stack = [];
        $hasDistinct = false;
        if (is_string($columns)) {
            $columns = [$columns];
        }
        foreach ($columns as $key => $value) {
            $isIntKey = is_int($key);
            $isArrayValue = is_array($value);
            if (!$isIntKey && $isArrayValue && $root && count(array_keys($columns)) === 1) {
                $stack[] = $this->columnQuote((string)$key);
                $stack[] = $this->columnPush($value, $map, false, $isJoin);
            } elseif ($isArrayValue) {
                $stack[] = $this->columnPush($value, $map, false, $isJoin);
            } elseif (!$isIntKey && $raw = $this->buildRaw($value, $map)) {
                preg_match('/(?<column>[\p{L}_][\p{L}\p{N}@$#\-_.]*)(\s*\[(?<type>(String|Bool|Int|Number))])?/u', (string)$key, $match);
                $stack[] = "{$raw} AS {$this->columnQuote($match['column'])}";
            } elseif ($isIntKey && is_string($value)) {
                if ($isJoin && str_contains($value, '*')) {
                    throw new InvalidArgumentException('Cannot use table.* to select all columns while joining table.');
                }
                preg_match('/(?<column>[\p{L}_][\p{L}\p{N}@$#\-_.]*)(?:\s*\((?<alias>[\p{L}_][\p{L}\p{N}@$#\-_]*)\))?(?:\s*\[(?<type>(String|Bool|Int|Number|Object|JSON))])?/u', $value, $match);
                if (!empty($match['alias'])) {
                    $columnString = "{$this->columnQuote($match['column'])} AS {$this->columnQuote($match['alias'])}";
                    $columns[$key] = $match['alias'];
                    if (!empty($match['type'])) {
                        $columns[$key] .= ' [' . $match['type'] . ']';
                    }
                } else {
                    $columnString = $this->columnQuote($match['column']);
                }
                if (!$hasDistinct && str_starts_with($value, '@')) {
                    $columnString = 'DISTINCT ' . $columnString;
                    $hasDistinct = true;
                    array_unshift($stack, $columnString);
                    continue;
                }
                $stack[] = $columnString;
            }
        }
        return implode(',', $stack);
    }

    /**
     * @throws Exception
     */
    protected function dataImplode(array $data, array &$map, string $conjunctor): string
    {
        $stack = [];
        foreach ($data as $key => $value) {
            $type = gettype($value);
            if (
                $type === 'array' &&
                preg_match("/^(AND|OR)(\s+#.*)?$/", $key, $relationMatch)
            ) {
                $stack[] = '(' . $this->dataImplode($value, $map, ' ' . $relationMatch[1]) . ')';
                continue;
            }
            $mapKey = $this->mapKey();
            $isIndex = is_int($key);
            preg_match(
                '/([\p{L}_][\p{L}\p{N}@$#\-_.]*)(\[(?<operator>.*)])?([\p{L}_][\p{L}\p{N}@$#\-_.]*)?/u',
                $isIndex ? $value : $key,
                $match
            );
            $column = $this->columnQuote($match[1]);
            $operator = $match['operator'] ?? null;
            if ($isIndex && isset($match[4]) && in_array($operator, ['>', '>=', '<', '<=', '=', '!='])) {
                $stack[] = "{$column} {$operator} " . $this->columnQuote($match[4]);
                continue;
            }
            if ($operator && $operator != '=') {
                if (in_array($operator, ['>', '>=', '<', '<='])) {
                    $condition = "{$column} {$operator} ";
                    if (is_numeric($value)) {
                        $condition .= $mapKey;
                        $map[$mapKey] = [$value, is_float($value) ? PDO::PARAM_STR : PDO::PARAM_INT];
                    } elseif ($raw = $this->buildRaw($value, $map)) {
                        $condition .= $raw;
                    } else {
                        $condition .= $mapKey;
                        $map[$mapKey] = [$value, PDO::PARAM_STR];
                    }
                    $stack[] = $condition;
                } elseif ($operator === '!') {
                    switch ($type) {
                        case 'NULL':
                            $stack[] = $column . ' IS NOT NULL';
                            break;
                        case 'array':
                            $placeholders = [];
                            foreach ($value as $index => $item) {
                                $stackKey = $mapKey . $index . '_i';
                                $placeholders[] = $stackKey;
                                $map[$stackKey] = $this->typeMap($item, gettype($item));
                            }
                            $stack[] = $column . ' NOT IN (' . implode(', ', $placeholders) . ')';
                            break;
                        case 'object':
                            if ($raw = $this->buildRaw($value, $map)) {
                                $stack[] = "{$column} != {$raw}";
                            }
                            break;
                        case 'integer':
                        case 'double':
                        case 'boolean':
                        case 'string':
                            $stack[] = "{$column} != {$mapKey}";
                            $map[$mapKey] = $this->typeMap($value, $type);
                            break;
                    }
                } elseif ($operator === '~' || $operator === '!~') {
                    if ($type !== 'array') {
                        $value = [$value];
                    }
                    $connector = ' OR ';
                    $data = array_values($value);
                    if (is_array($data[0])) {
                        if (isset($value['AND']) || isset($value['OR'])) {
                            $connector = ' ' . array_keys($value)[0] . ' ';
                            $value = $data[0];
                        }
                    }
                    $likeClauses = [];
                    foreach ($value as $index => $item) {
                        $item = strval($item);
                        if (!preg_match('/((?<!\\\)\[.+(?<!\\\)]|(?<!\\\)[*?!%#^_]|%.+|.+%)/', $item)) {
                            $item = '%' . $item . '%';
                        }
                        $likeClauses[] = $column . ($operator === '!~' ? ' NOT' : '') . " LIKE {$mapKey}L{$index}";
                        $map["{$mapKey}L{$index}"] = [$item, PDO::PARAM_STR];
                    }
                    $stack[] = '(' . implode($connector, $likeClauses) . ')';
                } elseif ($operator === '<>' || $operator === '><') {
                    if ($type === 'array') {
                        if ($operator === '><') {
                            $column .= ' NOT';
                        }
                        if ($this->isRaw($value[0]) && $this->isRaw($value[1])) {
                            $stack[] = "({$column} BETWEEN {$this->buildRaw($value[0], $map)} AND {$this->buildRaw($value[1], $map)})";
                        } else {
                            $stack[] = "({$column} BETWEEN {$mapKey}a AND {$mapKey}b)";
                            $dataType = (is_numeric($value[0]) && is_numeric($value[1])) ? PDO::PARAM_INT : PDO::PARAM_STR;
                            $map[$mapKey . 'a'] = [$value[0], $dataType];
                            $map[$mapKey . 'b'] = [$value[1], $dataType];
                        }
                    }
                } elseif ($operator === 'REGEXP') {
                    $stack[] = "{$column} REGEXP {$mapKey}";
                    $map[$mapKey] = [$value, PDO::PARAM_STR];
                } else {
                    throw new Exception("列 {$column} 的操作[{$operator}]不合法.");
                }
                continue;
            }
            switch ($type) {
                case 'NULL':
                    $stack[] = $column . ' IS NULL';
                    break;
                case 'array':
                    $placeholders = [];
                    foreach ($value as $index => $item) {
                        $stackKey = $mapKey . $index . '_i';
                        $placeholders[] = $stackKey;
                        $map[$stackKey] = $this->typeMap($item, gettype($item));
                    }
                    $stack[] = $column . ' IN (' . implode(', ', $placeholders) . ')';
                    break;
                case 'object':
                    if ($raw = $this->buildRaw($value, $map)) {
                        $stack[] = "{$column} = {$raw}";
                    }
                    break;
                case 'integer':
                case 'double':
                case 'boolean':
                case 'string':
                    $stack[] = "{$column} = {$mapKey}";
                    $map[$mapKey] = $this->typeMap($value, $type);
                    break;
            }
        }
        return implode($conjunctor . ' ', $stack);
    }

    /**
     * @throws Exception
     */
    protected function whereClause($where, array &$map): string
    {
        $clause = '';
        if (is_array($where)) {
            $conditions = array_diff_key($where, array_flip(
                ['GROUP', 'ORDER', 'HAVING', 'LIMIT', 'LIKE', 'MATCH']
            ));
            if (!empty($conditions)) {
                $clause = ' WHERE ' . $this->dataImplode($conditions, $map, ' AND');
            }

            if (isset($where['MATCH']) && $this->type === 'mysql') {
                $match = $where['MATCH'];
                if (is_array($match) && isset($match['columns'], $match['keyword'])) {
                    $mode = '';
                    $options = [
                        'natural' => 'IN NATURAL LANGUAGE MODE',
                        'natural+query' => 'IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION',
                        'boolean' => 'IN BOOLEAN MODE',
                        'query' => 'WITH QUERY EXPANSION'
                    ];
                    if (isset($match['mode'], $options[$match['mode']])) {
                        $mode = ' ' . $options[$match['mode']];
                    }
                    $columns = implode(', ', array_map([$this, 'columnQuote'], $match['columns']));
                    $mapKey = $this->mapKey();
                    $map[$mapKey] = [$match['keyword'], PDO::PARAM_STR];
                    $clause .= ($clause !== '' ? ' AND ' : ' WHERE') . ' MATCH (' . $columns . ') AGAINST (' . $mapKey . $mode . ')';
                }
            }
            if (isset($where['GROUP'])) {
                $group = $where['GROUP'];
                if (is_array($group)) {
                    $stack = [];
                    foreach ($group as $column => $value) {
                        $stack[] = $this->columnQuote($value);
                    }
                    $clause .= ' GROUP BY ' . implode(',', $stack);
                } elseif ($raw = $this->buildRaw($group, $map)) {
                    $clause .= ' GROUP BY ' . $raw;
                } else {
                    $clause .= ' GROUP BY ' . $this->columnQuote($group);
                }
            }
            if (isset($where['HAVING'])) {
                $having = $where['HAVING'];
                if ($raw = $this->buildRaw($having, $map)) {
                    $clause .= ' HAVING ' . $raw;
                } else {
                    $clause .= ' HAVING ' . $this->dataImplode($having, $map, ' AND');
                }
            }
            if (isset($where['ORDER'])) {
                $order = $where['ORDER'];
                if (is_array($order)) {
                    $stack = [];
                    foreach ($order as $column => $value) {
                        if (is_array($value)) {
                            $valueStack = [];
                            foreach ($value as $item) {
                                $valueStack[] = is_int($item) ? $item : $this->quote($item);
                            }
                            $valueString = implode(',', $valueStack);
                            $stack[] = "FIELD({$this->columnQuote($column)}, {$valueString})";
                        } elseif ($value === 'ASC' || $value === 'DESC') {
                            $stack[] = $this->columnQuote($column) . ' ' . $value;
                        } elseif (is_int($column)) {
                            $stack[] = $this->columnQuote($value);
                        }
                    }
                    $clause .= ' ORDER BY ' . implode(',', $stack);
                } elseif ($raw = $this->buildRaw($order, $map)) {
                    $clause .= ' ORDER BY ' . $raw;
                } else {
                    $clause .= ' ORDER BY ' . $this->columnQuote($order);
                }
            }
            if (isset($where['LIMIT'])) {
                $limit = $where['LIMIT'];
                if (in_array($this->type, ['oracle', 'mssql'])) {
                    if ($this->type === 'mssql' && !isset($where['ORDER'])) {
                        $clause .= ' ORDER BY (SELECT 0)';
                    }
                    if (is_numeric($limit)) {
                        $limit = [0, $limit];
                    }
                    if (
                        is_array($limit) &&
                        is_numeric($limit[0]) &&
                        is_numeric($limit[1])
                    ) {
                        $clause .= " OFFSET {$limit[0]} ROWS FETCH NEXT {$limit[1]} ROWS ONLY";
                    }
                } else {
                    if (is_numeric($limit)) {
                        $clause .= ' LIMIT ' . $limit;
                    } elseif (
                        is_array($limit) &&
                        is_numeric($limit[0]) &&
                        is_numeric($limit[1])
                    ) {
                        $clause .= " LIMIT {$limit[1]} OFFSET {$limit[0]}";
                    }
                }
            }
        } elseif ($raw = $this->buildRaw($where, $map)) {
            $clause .= ' ' . $raw;
        }
        return $clause;
    }

    /**
     * @throws Exception
     */
    protected function selectContext(
        string $table,
        array  &$map,
               $join,
               &$columns = null,
               $where = null,
               $columnFn = null
    ): string
    {
        preg_match('/(?<table>[\p{L}_][\p{L}\p{N}@$#\-_]*)\s*\((?<alias>[\p{L}_][\p{L}\p{N}@$#\-_]*)\)/u', $table, $tableMatch);

        if (isset($tableMatch['table'], $tableMatch['alias'])) {
            $table = $this->tableQuote($tableMatch['table']);
            $tableAlias = $this->tableQuote($tableMatch['alias']);
            $tableQuery = "{$table} AS {$tableAlias}";
        } else {
            $table = $this->tableQuote($table);
            $tableQuery = $table;
        }

        $isJoin = $this->isJoin($join);

        if ($isJoin) {
            $tableQuery .= ' ' . $this->buildJoin($tableAlias ?? $table, $join, $map);
        } else {
            if (is_null($columns)) {
                if (
                    !is_null($where) ||
                    (is_array($join) && isset($columnFn))
                ) {
                    $where = $join;
                    $columns = null;
                } else {
                    $where = null;
                    $columns = $join;
                }
            } else {
                $where = $columns;
                $columns = $join;
            }
        }

        if (isset($columnFn)) {
            if ($columnFn === 1) {
                $column = '1';

                if (is_null($where)) {
                    $where = $columns;
                }
            } elseif ($raw = $this->buildRaw($columnFn, $map)) {
                $column = $raw;
            } else {
                if (empty($columns) || $this->isRaw($columns)) {
                    $columns = '*';
                    $where = $join;
                }

                $column = $columnFn . '(' . $this->columnPush($columns, $map, true) . ')';
            }
        } else {
            $column = $this->columnPush($columns, $map, true, $isJoin);
        }

        return 'SELECT ' . $column . ' FROM ' . $tableQuery . $this->whereClause($where, $map);
    }

    protected function isJoin($join): bool
    {
        if (!is_array($join)) {
            return false;
        }

        $keys = array_keys($join);

        if (
            isset($keys[0]) &&
            is_string($keys[0]) &&
            str_starts_with($keys[0], '[')
        ) {
            return true;
        }

        return false;
    }

    /**
     * @throws Exception
     */
    protected function buildJoin(string $table, array $join, array &$map): string
    {
        $tableJoin = [];
        $type = [
            '>' => 'LEFT',
            '<' => 'RIGHT',
            '<>' => 'FULL',
            '><' => 'INNER'
        ];

        foreach ($join as $subtable => $relation) {
            preg_match('/(\[(?<join><>?|><?)])?(?<table>[\p{L}_][\p{L}\p{N}@$#\-_]*)\s?(\((?<alias>[\p{L}_][\p{L}\p{N}@$#\-_]*)\))?/u', $subtable, $match);

            if ($match['join'] === '' || $match['table'] === '') {
                continue;
            }

            if (is_string($relation)) {
                $relation = 'USING ("' . $relation . '")';
            } elseif (is_array($relation)) {
                // For ['column1', 'column2']
                if (isset($relation[0])) {
                    $relation = 'USING ("' . implode('", "', $relation) . '")';
                } else {
                    $joins = [];

                    foreach ($relation as $key => $value) {
                        if ($key === 'AND' && is_array($value)) {
                            $joins[] = $this->dataImplode($value, $map, ' AND');
                            continue;
                        }

                        $joins[] = (
                            strpos($key, '.') > 0 ?
                                // For ['tableB.column' => 'column']
                                $this->columnQuote($key) :

                                // For ['column1' => 'column2']
                                $table . '.' . $this->columnQuote($key)
                            ) .
                            ' = ' .
                            $this->tableQuote($match['alias'] ?? $match['table']) . '.' . $this->columnQuote($value);
                    }

                    $relation = 'ON ' . implode(' AND ', $joins);
                }
            } elseif ($raw = $this->buildRaw($relation, $map)) {
                $relation = $raw;
            }

            $tableName = $this->tableQuote($match['table']);

            if (isset($match['alias'])) {
                $tableName .= ' AS ' . $this->tableQuote($match['alias']);
            }

            $tableJoin[] = $type[$match['join']] . " JOIN {$tableName} {$relation}";
        }

        return implode(' ', $tableJoin);
    }

    protected function columnMap($columns, array &$stack, bool $root): array
    {
        if ($columns === '*') {
            return $stack;
        }

        foreach ($columns as $key => $value) {
            if (is_int($key)) {
                preg_match('/([\p{L}_][\p{L}\p{N}@$#\-_]*\.)?(?<column>[\p{L}_][\p{L}\p{N}@$#\-_]*)(?:\s*\((?<alias>[\p{L}_][\p{L}\p{N}@$#\-_]*)\))?(?:\s*\[(?<type>(String|Bool|Int|Number|Object|JSON))])?/u', $value, $keyMatch);

                $columnKey = !empty($keyMatch['alias']) ?
                    $keyMatch['alias'] :
                    $keyMatch['column'];

                $stack[$value] = isset($keyMatch['type']) ?
                    [$columnKey, $keyMatch['type']] :
                    [$columnKey];
            } elseif ($this->isRaw($value)) {
                preg_match('/([\p{L}_][\p{L}\p{N}@$#\-_]*\.)?(?<column>[\p{L}_][\p{L}\p{N}@$#\-_]*)(\s*\[(?<type>(String|Bool|Int|Number))])?/u', $key, $keyMatch);
                $columnKey = $keyMatch['column'];

                $stack[$key] = isset($keyMatch['type']) ?
                    [$columnKey, $keyMatch['type']] :
                    [$columnKey];
            } elseif (!is_int($key) && is_array($value)) {
                if ($root && count(array_keys($columns)) === 1) {
                    $stack[$key] = [$key, 'String'];
                }

                $this->columnMap($value, $stack, false);
            }
        }

        return $stack;
    }

    protected function dataMap(
        array $data,
        array $columns,
        array $columnMap,
        array &$stack,
        bool  $root,
        array &$result = null
    ): void
    {
        if ($root) {
            $columnsKey = array_keys($columns);

            if (count($columnsKey) === 1 && is_array($columns[$columnsKey[0]])) {
                $indexKey = array_keys($columns)[0];
                $dataKey = preg_replace("/^[\p{L}_][\p{L}\p{N}@$#\-_]*\./u", '', $indexKey);
                $currentStack = [];

                foreach ($data as $item) {
                    $this->dataMap($data, $columns[$indexKey], $columnMap, $currentStack, false, $result);
                    $index = $data[$dataKey];

                    if (isset($result)) {
                        $result[$index] = $currentStack;
                    } else {
                        $stack[$index] = $currentStack;
                    }
                }
            } else {
                $currentStack = [];
                $this->dataMap($data, $columns, $columnMap, $currentStack, false, $result);

                if (isset($result)) {
                    $result[] = $currentStack;
                } else {
                    $stack = $currentStack;
                }
            }

            return;
        }

        foreach ($columns as $key => $value) {
            $isRaw = $this->isRaw($value);

            if (is_int($key) || $isRaw) {
                $map = $columnMap[$isRaw ? $key : $value];
                $columnKey = $map[0];
                $item = $data[$columnKey];

                if (isset($map[1])) {
                    if ($isRaw && in_array($map[1], ['Object', 'JSON'])) {
                        continue;
                    }

                    if (is_null($item)) {
                        $stack[$columnKey] = null;
                        continue;
                    }

                    switch ($map[1]) {

                        case 'Number':
                            $stack[$columnKey] = (float)$item;
                            break;

                        case 'Int':
                            $stack[$columnKey] = (int)$item;
                            break;

                        case 'Bool':
                            $stack[$columnKey] = (bool)$item;
                            break;

                        case 'Object':
                            $stack[$columnKey] = unserialize($item);
                            break;

                        case 'JSON':
                            $stack[$columnKey] = json_decode($item, true);
                            break;

                        case 'String':
                            $stack[$columnKey] = (string)$item;
                            break;
                    }
                } else {
                    $stack[$columnKey] = $item;
                }
            } else {
                $currentStack = [];
                $this->dataMap($data, $value, $columnMap, $currentStack, false, $result);

                $stack[$key] = $currentStack;
            }
        }
    }

    /**
     * @throws Exception
     */
    public function create(string $table, $columns, $options = null): ?PDOStatement
    {
        $stack = [];
        $tableOption = '';
        $tableName = $this->tableQuote($table);

        foreach ($columns as $name => $definition) {
            if (is_int($name)) {
                $stack[] = preg_replace('/<([\p{L}_][\p{L}\p{N}@$#\-_]*)>/u', '"$1"', $definition);
            } elseif (is_array($definition)) {
                $stack[] = $this->columnQuote($name) . ' ' . implode(' ', $definition);
            } elseif (is_string($definition)) {
                $stack[] = $this->columnQuote($name) . ' ' . $definition;
            }
        }

        if (is_array($options)) {
            $optionStack = [];

            foreach ($options as $key => $value) {
                if (is_string($value) || is_int($value)) {
                    $optionStack[] = "{$key} = {$value}";
                }
            }

            $tableOption = ' ' . implode(', ', $optionStack);
        } elseif (is_string($options)) {
            $tableOption = ' ' . $options;
        }

        $command = 'CREATE TABLE';

        if (in_array($this->type, ['mysql', 'pgsql', 'sqlite'])) {
            $command .= ' IF NOT EXISTS';
        }

        return $this->exec("{$command} {$tableName} (" . implode(', ', $stack) . "){$tableOption}");
    }

    /**
     * @throws Exception
     */
    public function drop(string $table): ?PDOStatement
    {
        return $this->exec('DROP TABLE IF EXISTS ' . $this->tableQuote($table));
    }

    /**
     * @throws Exception
     */
    public function select(string $table, $join, $columns = null, $where = null): ?array
    {
        $map = [];
        $result = [];
        $columnMap = [];

        $args = func_get_args();
        $lastArgs = $args[array_key_last($args)];
        $callback = is_callable($lastArgs) ? $lastArgs : null;

        $where = is_callable($where) ? null : $where;
        $columns = is_callable($columns) ? null : $columns;

        $column = $where === null ? $join : $columns;
        $isSingle = (is_string($column) && $column !== '*');

        $statement = $this->exec($this->selectContext($table, $map, $join, $columns, $where), $map);

        $this->columnMap($columns, $columnMap, true);

        if (!$this->statement) {
            return $result;
        }

        if ($columns === '*') {
            if (isset($callback)) {
                while ($data = $statement->fetch(PDO::FETCH_ASSOC)) {
                    $callback($data);
                }

                return null;
            }

            return $statement->fetchAll(PDO::FETCH_ASSOC);
        }

        while ($data = $statement->fetch(PDO::FETCH_ASSOC)) {
            $currentStack = [];

            if (isset($callback)) {
                $this->dataMap($data, $columns, $columnMap, $currentStack, true);

                $callback(
                    $isSingle ?
                        $currentStack[$columnMap[$column][0]] :
                        $currentStack
                );
            } else {
                $this->dataMap($data, $columns, $columnMap, $currentStack, true, $result);
            }
        }

        if (isset($callback)) {
            return null;
        }

        if ($isSingle) {
            $singleResult = [];
            $resultKey = $columnMap[$column][0];

            foreach ($result as $item) {
                $singleResult[] = $item[$resultKey];
            }

            return $singleResult;
        }

        return $result;
    }

    /**
     * @throws Exception
     */
    public function insert(string $table, array $values)
    {
        $stack = [];
        $columns = [];
        $fields = [];
        $map = [];
        $returnings = [];

        if (!isset($values[0])) {
            $values = [$values];
        }

        foreach ($values as $data) {
            foreach ($data as $key => $value) {
                $columns[] = $key;
            }
        }

        $columns = array_unique($columns);

        foreach ($values as $data) {
            $values = [];

            foreach ($columns as $key) {
                $value = $data[$key];
                $type = gettype($value);

                if ($this->type === 'oracle' && $type === 'resource') {
                    $values[] = 'EMPTY_BLOB()';
                    $returnings[$this->mapKey()] = [$key, $value, PDO::PARAM_LOB];
                    continue;
                }

                if ($raw = $this->buildRaw($data[$key], $map)) {
                    $values[] = $raw;
                    continue;
                }

                $mapKey = $this->mapKey();
                $values[] = $mapKey;

                switch ($type) {
                    case 'array':
                        $map[$mapKey] = [
                            strpos($key, '[JSON]') === strlen($key) - 6 ?
                                json_encode($value) :
                                serialize($value),
                            PDO::PARAM_STR
                        ];
                        break;
                    case 'object':
                        $value = serialize($value);
                        break;

                    case 'NULL':
                    case 'resource':
                    case 'boolean':
                    case 'integer':
                    case 'double':
                    case 'string':
                        $map[$mapKey] = $this->typeMap($value, $type);
                        break;
                }
            }

            $stack[] = '(' . implode(', ', $values) . ')';
        }
        foreach ($columns as $key) {
            $fields[] = $this->columnQuote(preg_replace("/(\s*\[JSON]$)/i", '', $key));
        }
        $query = 'INSERT INTO ' . $this->tableQuote($table) . ' (' . implode(', ', $fields) . ') VALUES ' . implode(', ', $stack);
        return $this->exec($query, $map);
    }

    /**
     * @throws Exception
     */
    public function update($table, $data, $where = null): int
    {
        try {
            $fields = [];
            $map = [];
            foreach ($data as $key => $value) {
                $column = $this->columnQuote(preg_replace('/(\\s*\\[(JSON|\\+|-|\\*|\\/)]$)/i', '', $key));
                if ($raw = $this->buildRaw($value, $map)) {
                    $fields[] = $column . ' = ' . $raw;
                    continue;
                }
                $map_key = $this->mapKey();
                preg_match('#(?<column>[a-zA-Z0-9_]+)(\[(?<operator>[+\-*/])])?#i', $key, $match);
                if (isset($match['operator'])) {
                    if (is_numeric($value)) {
                        $fields[] = $column . ' = ' . $column . ' ' . $match['operator'] . ' ' . $value;
                    }
                } else {
                    $fields[] = $column . ' = ' . $map_key;
                    $type = gettype($value);
                    switch ($type) {
                        case 'array':
                            $map[$map_key] = [
                                strpos($key, '[JSON]') === strlen($key) - 6
                                    ?
                                    json_encode($value)
                                    :
                                    serialize($value),
                                PDO::PARAM_STR,
                            ];
                            break;
                        case 'object':
                            $value = serialize($value);
                        case 'NULL':
                        case 'resource':
                        case 'boolean':
                        case 'integer':
                        case 'double':
                        case 'string':
                            $map[$map_key] = $this->typeMap($value, $type);
                            break;
                        default:
                            break;
                    }
                }
            }
            return $this->exec('UPDATE ' . $this->tableQuote($table) . ' SET ' . implode(', ', $fields) . $this->whereClause($where, $map), $map)->rowCount();
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function delete($table, $where): int
    {
        try {
            $map = [];
            return $this->exec('DELETE FROM ' . $this->tableQuote($table) . $this->whereClause($where, $map), $map)->rowCount();
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function replace(string $table, array $columns, $where = null): ?PDOStatement
    {
        $map = [];
        $stack = [];

        foreach ($columns as $column => $replacements) {
            if (is_array($replacements)) {
                foreach ($replacements as $old => $new) {
                    $mapKey = $this->mapKey();
                    $columnName = $this->columnQuote($column);
                    $stack[] = "{$columnName} = REPLACE({$columnName}, {$mapKey}a, {$mapKey}b)";

                    $map[$mapKey . 'a'] = [$old, PDO::PARAM_STR];
                    $map[$mapKey . 'b'] = [$new, PDO::PARAM_STR];
                }
            }
        }

        if (empty($stack)) {
            throw new Exception('列非法.');
        }

        return $this->exec('UPDATE ' . $this->tableQuote($table) . ' SET ' . implode(', ', $stack) . $this->whereClause($where, $map), $map);
    }

    /**
     * @throws Exception
     */
    public function get(string $table, $join = null, $columns = null, $where = null): array|bool|string|null|int
    {
        try {
            $map = [];
            $result = [];
            $columnMap = [];
            $currentStack = [];
            if ($where === null) {
                if ($this->isJoin($join)) {
                    $where['LIMIT'] = 1;
                } else {
                    $columns['LIMIT'] = 1;
                }
                $column = $join;
            } else {
                $column = $columns;
                $where['LIMIT'] = 1;
            }
            $isSingle = (is_string($column) && $column !== '*');
            $query = $this->exec($this->selectContext($table, $map, $join, $columns, $where), $map);

            if (!$this->statement) {
                return false;
            }
            $data = $query->fetchAll(PDO::FETCH_ASSOC);
            if (isset($data[0])) {
                if ($column === '*') {
                    return $data[0];
                }
                $this->columnMap($columns, $columnMap, true);
                $this->dataMap($data[0], $columns, $columnMap, $currentStack, true, $result);
                if ($isSingle) {
                    return $result[0][$columnMap[$column][0]];
                }
                return $result[0];
            }
            return [];
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function has(string $table, $join, $where = null): bool
    {
        $map = [];
        $column = null;

        $query = $this->exec(
            $this->type === 'mssql' ?
                $this->selectContext($table, $map, $join, $column, $where, self::raw('TOP 1 1')) :
                'SELECT EXISTS(' . $this->selectContext($table, $map, $join, $column, $where, 1) . ')',
            $map
        );

        if (!$this->statement) {
            return false;
        }

        $result = $query->fetchColumn();

        return $result === '1' || $result === 1 || $result === true;
    }

    /**
     * @throws Exception
     */
    public function rand(string $table, $join = null, $columns = null, $where = null): array
    {
        $orderRaw = $this->raw(
            $this->type === 'mysql' ? 'RAND()'
                : ($this->type === 'mssql' ? 'NEWID()'
                : 'RANDOM()')
        );

        if ($where === null) {
            if ($this->isJoin($join)) {
                $where['ORDER'] = $orderRaw;
            } else {
                $columns['ORDER'] = $orderRaw;
            }
        } else {
            $where['ORDER'] = $orderRaw;
        }

        return $this->select($table, $join, $columns, $where);
    }

    /**
     * @throws Exception
     */
    private function aggregate(string $type, string $table, $join = null, $column = null, $where = null): ?string
    {
        $map = [];
        $query = $this->exec($this->selectContext($table, $map, $join, $column, $where, $type), $map);
        if (!$this->statement) {
            return null;
        }
        return (string)$query->fetchColumn();
    }

    /**
     * @throws Exception
     */
    public function count(string $table, $join = null, $column = null, $where = null): ?int
    {
        return (int)$this->aggregate('COUNT', $table, $join, $column, $where);
    }

    /**
     * @throws Exception
     */
    public function avg(string $table, $join, $column = null, $where = null): ?string
    {
        return $this->aggregate('AVG', $table, $join, $column, $where);
    }

    /**
     * @throws Exception
     */
    public function max(string $table, $join, $column = null, $where = null): ?string
    {
        return $this->aggregate('MAX', $table, $join, $column, $where);
    }

    /**
     * @throws Exception
     */
    public function min(string $table, $join, $column = null, $where = null): ?string
    {
        return $this->aggregate('MIN', $table, $join, $column, $where);
    }

    /**
     * @throws Exception
     */
    public function sum(string $table, $join, $column = null, $where = null): ?string
    {
        return $this->aggregate('SUM', $table, $join, $column, $where);
    }

    /**
     * @throws Exception
     */
    public function action(callable $actions)
    {
        try {
            if (is_callable($actions)) {
                $this->beginTransaction();
                try {
                    $result = $actions($this);
                    if ($result === false) {
                        $this->rollBack();
                    } else {
                        $this->commit();
                    }
                } catch (Exception $th) {
                    $this->rollBack();
                    throw new Exception($th->getMessage());
                }
                return $result;
            }
            return false;
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function beginTransaction(): void
    {
        try {
            if ($this->inTransaction) {
                throw new Exception('当前已开启事物');
            }
            $this->realGetConn();
            $this->pdo->beginTransaction();
            $this->inTransaction = true;
            Coroutine::defer(function () {
                if ($this->inTransaction) {
                    $this->rollBack();
                }
            });
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    public function rollBack(): void
    {
        try {
            $this->pdo->rollBack();
            $this->inTransaction = false;
            $this->release($this->pdo);
        } catch (Throwable $th) {
            var_dump($th->getMessage());
        }
    }

    /**
     * @throws Exception
     */
    public function commit(): void
    {
        try {
            $this->pdo->commit();
            $this->inTransaction = false;
            $this->release($this->pdo);
        } catch (Throwable $th) {
            var_dump($th->getMessage());
        }
    }

    public function id(string $name = null): ?string
    {
        $type = $this->type;

        if ($type === 'oracle') {
            return $this->returnId;
        } elseif ($type === 'pgsql') {
            $id = $this->pdo->query('SELECT LASTVAL()')->fetchColumn();

            return (string)$id ?: null;
        }

        return $this->pdo->lastInsertId($name);
    }

    public function debug(): self
    {
        $this->debugMode = true;
        return $this;
    }

    public function last(): ?string
    {
        if (empty($this->logs)) {
            return null;
        }
        $log = $this->logs[array_key_last($this->logs)];
        return $this->generate($log[0], $log[1]);
    }

    public function log(): array
    {
        return array_map(
            function ($log) {
                return $this->generate($log[0], $log[1]);
            },
            $this->logs
        );
    }

    public function info(): array
    {
        $output = [
            'server' => 'SERVER_INFO',
            'driver' => 'DRIVER_NAME',
            'client' => 'CLIENT_VERSION',
            'version' => 'SERVER_VERSION',
            'connection' => 'CONNECTION_STATUS'
        ];

        foreach ($output as $key => $value) {
            try {
                $output[$key] = $this->pdo->getAttribute(constant('PDO::ATTR_' . $value));
            } catch (PDOException $e) {
                $output[$key] = $e->getMessage();
            }
        }

        $output['dsn'] = $this->dsn;

        return $output;
    }

    /**
     * @throws Exception
     */
    public function release($connection = null): bool
    {
        if ($connection === null) {
            $this->inTransaction = false;
        }
        if (!$this->inTransaction) {
            $this->pool->close($connection);
            return true;
        }
        return false;
    }

    /**
     * @throws Exception
     */
    private function realGetConn(): void
    {
        if (!$this->inTransaction) {
            $this->pdo = $this->pool->getConnection();
            $this->pdo->exec('SET SQL_MODE=ANSI_QUOTES');
        }
    }
}