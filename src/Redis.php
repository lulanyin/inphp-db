<?php
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

    public static function newRedis($int = 0){
        $config = DB::getConfig();
        $config = $config['redis'] ?? [];
        $config['select'] = $int;
        return new Cache\driver\Redis($config);
    }
}