<?php


namespace App\HttpController;

use App\Extend\Validate\Validate;
use App\Util\SystemUtil;
use Throwable;
use SQLite3;

class System extends \App\Middleware\SystemMiddleware
{

    public function login(): void
    {
        try {
            $params = $this->request()->getAttribute('request_params', []);
            Validate::Check($params, [
                ['username', 'required'],
                ['password', 'required']
            ]);
            $db = new SQLite3(EASYSWOOLE_ROOT . '/system.db');
            $result = $db->query("SELECT * FROM config WHERE id = 1");
            $row = $result->fetchArray(SQLITE3_ASSOC);
            if ($row['username'] != $params['username']) {
                SystemUtil::returnJson($this->response(), 1, '错误');
            }
            if ($row['password'] != strtoupper(md5($params['password']))) {
                SystemUtil::returnJson($this->response(), 1, '错误');
            }
            $token = md5($params['username'] . rand(1111, 9999) . $params['password'] . time());
            $db->exec("UPDATE config SET token = '{$token}' WHERE id = 1");
            SystemUtil::returnJson($this->response(), 0, '', [
                'token' => $token
            ]);
        } catch (Throwable $th) {
            SystemUtil::returnJson($this->response(), $th->getCode() ?? 1, $th->getMessage());
        }
    }

    public function info(): void
    {
        try {
            $params = $this->request()->getAttribute('request_params', []);
            Validate::Check($params, [
                ['token', 'required']
            ]);
            $db = new SQLite3(EASYSWOOLE_ROOT . '/system.db');
            $result = $db->query("SELECT * FROM config WHERE id = 1");
            $row = $result->fetchArray(SQLITE3_ASSOC);
            if ($row['token'] != $params['token']) {
                SystemUtil::returnJson($this->response(), 1, '错误');
            }
            SystemUtil::returnJson($this->response(), 0, '', [
                'info' => [
                    'username' => $row['username'],
                    'port' => $row['port'],
                    'servername' => $row['servername'],
                ]
            ]);
        } catch (Throwable $th) {
            SystemUtil::returnJson($this->response(), $th->getCode() ?? 1, $th->getMessage());
        }
    }

    public function tree(): void
    {
        try {
            $params = $this->request()->getAttribute('request_params', []);
            Validate::Check($params, [
                ['token', 'required']
            ]);
            $db = new SQLite3(EASYSWOOLE_ROOT . '/system.db');
            $result = $db->query("SELECT * FROM config WHERE id = 1");
            $row = $result->fetchArray(SQLITE3_ASSOC);
            if ($row['token'] != $params['token']) {
                SystemUtil::returnJson($this->response(), 1, '错误');
            }
            $config = include EASYSWOOLE_ROOT.'/dev.php';
            var_export($config);
            $tree = self::getTree(EASYSWOOLE_ROOT . '/App/');
            SystemUtil::returnJson($this->response(), 0, '', [
                'tree' => $tree
            ]);
        } catch (Throwable $th) {
            SystemUtil::returnJson($this->response(), $th->getCode() ?? 1, $th->getMessage());
        }
    }

    private function getTree($dir)
    {
        $result = [];
        if (is_dir($dir)) {
            $files = scandir($dir);
            foreach ($files as $file) {
                if ($file === '.' || $file === '..') {
                    continue;
                }
                $fullPath = $dir . DIRECTORY_SEPARATOR . $file;
                if (is_dir($fullPath)) {
                    $result[$file] = self::getTree($fullPath);
                } elseif (pathinfo($file, PATHINFO_EXTENSION) === 'php') {
                    $result[] = $file;
                }
            }
        }
        return $result;
    }
}
