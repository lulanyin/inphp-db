<?php
namespace Inphp\DB;

use Inphp\DB\Cache\driver\Redis;

class Cache{

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
        return self::getConnection($int);
    }

    /**
     * 创建连接
     * @param int $int
     * @return Redis
     */
    public static function connect($int = null){
        return self::getConnection($int);
    }

    /**
     * @param int|null $int
     * @return Redis
     */
    public static function getConnection($int = null){
        $config = DB::getConfig();
        $int = is_null($int) ? self::$active : $int;
        $int = is_null($int) ? $config['redis']['select'] : $int;
        self::$active = $int;
        return \Inphp\DB\Redis::get($int);
    }
}