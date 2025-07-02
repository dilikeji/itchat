<?php

namespace App\Util;

use EasySwoole\Component\Context\ContextManager;
use EasySwoole\EasySwoole\Logger;
use EasySwoole\Http\Request;
use EasySwoole\Http\Response;
use EasySwoole\Utility\SnowFlake;
use Exception;
use Throwable;
use voku\helper\AntiXSS;

class SystemUtil
{
    public static function millisecond(): int
    {
        $a = explode(' ', microtime());
        return intval(bcmul(1000, bcadd($a[0], $a[1], 3)));
    }

    public static function getClientIp(Request $request): string
    {
        $ip = $request->getHeader('x-real-ip');
        if (empty($ip)) {
            $ip = $request->getHeader('remote-addr');
        }
        if (empty($ip)) {
            $ip = $request->getHeader('remote_addr');
        }
        if (empty($ip)) {
            $ip = $request->getHeader('x_real_ip');
        }
        if (empty($ip)) {
            $ip = $request->getHeader('remote_ip');
        }
        if (empty($ip)) {
            $ip = $request->getHeader('remote-ip');
        }
        if (empty($ip)) {
            $ip = $request->getSwooleRequest()->server['remote_addr'];
        }
        if (is_array($ip)) {
            return $ip[0];
        }
        return $ip;
    }

    /**
     * @throws Exception
     */
    public static function download(Response $response, string $path): void
    {
        try {
            if (!file_exists($path)) {
                $response->withStatus(404);
                $response->write('404');
            }
            $fileName = pathinfo($path, PATHINFO_FILENAME);
            $fileExtNameSub = pathinfo($path, PATHINFO_EXTENSION);
            $fileName .= '.' . $fileExtNameSub;
            $response->withHeader('Content-type', 'application/octet-stream');
            $response->withHeader('Last-Modified', gmdate('D, d M Y H:i:s') . ' GMT');
            $response->withHeader('Content-Transfer-Encoding', 'binary');
            $response->withHeader('Content-length', filesize($path));
            $response->withHeader('Content-Disposition', "filename= {$fileName}");
            $response->withStatus(200);
            $response->write(file_get_contents($path));
            $response->end();
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    public static function checkXss($value, bool $isClean = false)
    {
        $antiXss = new AntiXSS();
        $harmless_string = $antiXss->xss_clean($value);
        if ($isClean) {
            return $harmless_string;
        }
        return $antiXss->isXssFound();
    }

    public static function signStr(array $data, string $key = ''): string
    {
        $string = '';
        ksort($data);
        foreach ($data as $k => $v) {
            if (is_array($v)) {
                $string .= self::signStr($v);
                continue;
            }
            if (($v || $v === 0 || $v === '0' || $v === false) && $k !== 'sign') {
                $string .= "$k=$v&";
            }
        }
        if (empty($key)) {
            return $string;
        }
        $signStr = $string . 'key=' . $key;
        return strtoupper(md5($signStr));
    }

    /**
     * @throws Exception
     */
    public static function UUID(int $dataCenterId = 0, int $workerId = 0): string
    {
        try {
            return (new SnowFlake())->make($dataCenterId, $workerId);
        } catch (Throwable $th) {
            throw new Exception($th->getMessage());
        }
    }

    public static function getParams(): array
    {
        return ContextManager::getInstance()->get('REQUEST_') ?? [];
    }

    public static function ParamsTrim(array $params): array
    {
        foreach ($params as $k => $v) {
            if (is_array($v)) {
                $params[$k] = self::ParamsTrim($v);
                continue;
            }
            if (trim($v) === '') {
                unset($params[$k]);
            } else {
                $params[$k] = trim($v);
            }
        }
        return $params;
    }

    public static function returnJson(Response $response, int $code = 0, string $message = '', array $data = []): void
    {
        if (!$response->isEndResponse()) {
            if ($code) {
                Logger::getInstance()->error($message, 'Request');
            }
            if (stripos($message, 'Duplicate') !== false) {
                $message = '数据库唯一值重复';
            }
            $result = [
                'code' => $code,
                'msg' => $message,
                'data' => $data
            ];
            $response->withHeader('Content-type', 'application/json;charset=utf-8');
            $response->withStatus(200);
            try {
                $response->write(json_encode($result, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
            } catch (Throwable) {
                $response->write('{"code":1,"msg":"error","data":[]}');
            }
            $response->end();
        }
    }

    public static function With($db,&$list,$key,$key_other,$data_base_other,$other_list,$other_where): void
    {
        if(!empty($list)){
            $keyList=array_column($list,$key);
            if(!empty($keyList)){
                $key_list=array_keys($other_list);
                $key_list[]=$key_other;
                $other_where[$key_other]=$keyList;
                $list_other=$db->select($data_base_other,$key_list,$other_where);
                $KTV=[];
                foreach ($list_other as $v){
                    $KTV[$v[$key_other]]=$v;
                }
                foreach ($list as &$v){
                    foreach ($other_list as $kk=>$vv){
                        $v[$vv]=$KTV[$v[$key]][$kk];
                    }
                }
                unset($v);
            }
        }
    }
}