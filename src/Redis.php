<?php
// +----------------------------------------------------------------------
// | INPHP
// +----------------------------------------------------------------------
// | Copyright (c) 2020 https://inphp.cc All rights reserved.
// +----------------------------------------------------------------------
// | Licensed ( https://opensource.org/licenses/MIT )
// +----------------------------------------------------------------------
// | Author: lulanyin <me@lanyin.lu>
// +----------------------------------------------------------------------
namespace Inphp\DB;

class Redis{

    /**
     * @var Cache\driver\Redis[]
     */
    private static $redis = [];

    /**
     * @param $int
     * @return Cache\driver\Redis
     */
    public static function init($int = 0){
        if(!isset(self::$redis[$int])){
            self::$redis[$int] = self::newRedis($int);
        }
        return self::$redis[$int];
    }

    /**
     * 获取redis操作对象
     * @param int $int
     * @return Cache\driver\Redis
     */
    public static function get($int = 0){
        return self::init($int);
    }

    public static function newRedis($int = 0){
        $config = DB::getConfig();
        $config = $config['redis'] ?? [];
        $config['select'] = $int;
        return new Cache\driver\Redis($config);
    }
}