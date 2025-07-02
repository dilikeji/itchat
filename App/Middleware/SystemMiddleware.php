<?php

namespace App\Middleware;

use App\Extend\Redis\Redis;
use App\Util\SystemUtil;
use EasySwoole\Http\AbstractInterface\Controller;
use EasySwoole\Utility\Str;
use Exception;
use Throwable;

class SystemMiddleware extends Controller
{
    protected function onRequest(?string $action): ?bool
    {
        try {
            //获取、解析请求token
            $header = $this->request()->getHeaders();
            $token = [];
            if (!empty($header['authorization']) && !empty($header['authorization'][0])) {
                try {
                    $redis = new Redis();
                    $result = $redis->getValue($header['authorization'][0]);
                    $token = json_decode($result, true);
                } catch (Throwable) {
                    throw new Exception('token失效,请重新登录', 10);
                }
            }
            //获取、过滤、存储请求参数
            $postJson = [];
            if ($this->request()->getMethod() === 'POST') {
                if (Str::contains($header['content-type'][0], 'application/json', false)) {
                    $postStr = $this->request()->getSwooleRequest()->rawContent();
                    try {
                        $postJson = json_decode($postStr, true, 512, JSON_THROW_ON_ERROR);
                    } catch (Throwable) {
                        throw new Exception('raw非Json格式', 1);
                    }
                }
            }
            $getJson = $this->request()->getRequestParam();
            $params = [];
            foreach ($getJson as $k => $v) {
                $params[$k] = $v;
            }
            foreach ($postJson as $k => $v) {
                $params[$k] = $v;
            }
            foreach ($token as $k => $v) {
                $params[$k] = $v;
            }
            $params = SystemUtil::ParamsTrim(SystemUtil::checkXss($params, true));
            $this->request()->withAttribute('request_params', $params);
            if (isset($redis) && isset($result)) {
                $redis->setValue($header['authorization'][0], $result, 86400);
            }
            return true;
        } catch (Throwable $th) {
            SystemUtil::returnJson($this->response(), $th->getCode(), $th->getMessage());
            return false;
        }
    }

    protected function actionNotFound(?string $action): void
    {
        $this->response()->withStatus(404);
        $this->response()->write('404');
    }
}