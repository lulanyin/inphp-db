<?php
namespace Small\DB;

use Small\DB\Cache\driver\Redis;

class Cache{

    /**
     * @var Redis[]
     */
    private static $redisList = [];

    /**
     * 默认激活的连接
     * @var null
     */
    public static $active = null;

    /**
     * 获取缓存
     * @param $key
     * @param null $default
     * @return mixed
     */
    public static function get($key, $default = null){
        if($redis = self::getConnection(self::$active)) {
            return $redis->get($key, $default);
        }
        return null;
    }

    /**
     * 设置缓存
     * @param $key
     * @param $value
     * @param int $expire
     * @return bool
     */
    public static function set($key, $value, $expire = 86400){
        if($redis = self::getConnection(self::$active)){
            return $redis->set($key, $value, $expire);
        }
        return null;
    }

    public static function remove($key){
        if($redis = self::getConnection(self::$active)){
            return $redis->rm($key);
        }
        return null;
    }

    public static function update(string $key, array $value, $unique = false){
        if($cache = self::get($key)){
            $cache = is_array($cache) ? $cache : null;
            if(null !== $cache){
                $cache = array_merge($cache, $value);
                $cache = $unique ? array_unique($cache) : $cache;
                self::set($key, $cache);
            }
        }else{
            self::set($key, $value);
        }
    }

    public static function clear(){
        if($redis = self::getConnection(self::$active)){
            $redis->clear();
        }
    }

    /**
     * @param int $int
     * @return Redis
     */
    public static function select($int = null){
        return self::getConnection($int ?? self::$active);
    }

    /**
     * 创建连接
     * @param int $int
     * @return Redis
     */
    public static function connect($int = 0){
        self::$redisList[$int] = \Small\DB\Redis::newRedis($int);
        if(!self::$active){
            self::$active = $int;
        }
        return self::$redisList[$int];
    }

    /**
     * @param int|null $int
     * @return Redis
     */
    public static function getConnection($int = null){
        $int = is_null($int) ? self::$active : $int;
        $int = is_null($int) ? 0 : $int;
        //echo $int.PHP_EOL;
        if(!isset(self::$redisList[$int])){
            self::$redisList[$int] = \Small\DB\Redis::newRedis($int);
        }
        return self::$redisList[$int];
    }
}