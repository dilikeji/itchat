<?php

namespace App\Extend\Validate;

use Exception;
use Inhere\Validate\Validation;

class Validate
{

    /**
     * @throws Exception
     */
    public static function Check(array $data, array $rule): void
    {
        foreach ($rule as &$v){
            if($v[1]=='required'){
                $v['msg']='{attr}必填';
            }else{
                $v['msg']='{attr}格式错误';
            }
        }
        unset($v);
        $check = Validation::check($data, $rule);
        if ($check->isFail()) {
            throw new Exception($check->firstError());
        }
    }

    public static function Filtration(array $data, array $message): array
    {
        $returnData = [];
        foreach ($message as $k => $v) {
            if (!empty($data[$k])) {
                $returnData[$k] = $data[$k];
            }
        }
        return $returnData;
    }
}