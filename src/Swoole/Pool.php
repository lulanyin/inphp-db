<?php
/**
 * Create By Hunter
 * 2019/10/6 00:54:24
 */
namespace Small\DB\Swoole;

use Swoole\Coroutine\Channel;

class Pool
{
    /**
     * 连接池默认数量
     * @var int
     */
    private $length = 10;

    /**
     * @var Channel
     */
    private $channels;

    /**
     * 请在协程中初始化，否则无法使用
     * 初始化连接池
     * Pool constructor.
     * @param int $size 每个 Worker 最多可使用的连接数据
     */
    public function __construct($size = 5)
    {
        $this->length = $size;
        $this->putConnect();
    }

    public function putConnect(){
        $this->channels = new Channel($this->length);
        for($i = 0; $i < $this->length; $i ++){
            $this->put($this->connect());
        }
    }

    /**
     * 回收连接
     * @param Connection $connection
     */
    public function put(Connection $connection){
        $this->channels->push($connection);
    }

    /**
     * 获取连接
     * @return bool|Connection
     */
    public function get(){
        $mysql = $this->channels->pop(3);
        if($mysql == null){
            return null;
        }
        return $mysql;
    }

    /**
     * @return Connection
     */
    private function connect(){
        return new Connection(DB::getConfig());
    }


    /**
     * @var Pool
     */
    public static $MysqlPool = null;

    /**
     * 获取连接池
     * @return Connection
     */
    public static function getPool(){
        if(null == self::$MysqlPool){
            self::init();
        }
        return self::$MysqlPool->get();
    }

    /**
     * 释放回去
     * @param Connection $connection
     */
    public static function putPool(Connection $connection){
        if(null !== self::$MysqlPool){
            //如果回收的时候，事务还在进行，需要把事务提交了
            if($connection->inTransaction){
                $connection->getPdo('write')->commit();
            }
            self::$MysqlPool->put($connection);
        }
    }

    /**
     * 初始化
     */
    public static function init(){
        if(null == self::$MysqlPool){
            $size = defined("DB_SWOOLE_POOLS") ? DB_SWOOLE_POOLS : 5;
            $size = is_numeric($size) && $size>0 ? intval($size) : 5;
            self::$MysqlPool = new Pool($size);
        }
    }
}