<?php
/**
 * Create By Hunter
 * 2019/10/6 01:29:54
 */
namespace Small\DB\Swoole;

use Swoole\Coroutine;

class DB extends \Small\DB\DB
{

    /**
     * 查询开始
     * @param $tableName
     * @param null $as
     * @return \Small\DB\Swoole\Query
     */
    public static function from($tableName, $as = null){
        return (new Query())->from($tableName, $as);
    }

    /**
     * 连接池
     * @var Connection[]
     */
    public static $connections = [];

    /**
     * @return Connection
     */
    public static function getConnection()
    {
        $cid = Coroutine::getCid();
        if(isset(self::$connections[$cid]) && self::$connections[$cid] instanceof Connection){
            return self::$connections[$cid];
        }
        self::$connections[$cid] = Pool::getPool();
        //协程退出时会执行
        Coroutine::defer(function () use ($cid){
            unset(self::$connections[$cid]);
        });
        return self::$connections[$cid];
    }

    /**
     * 执行SQL
     * @param $sql
     * @param array $params
     * @return bool|mixed
     */
    public static function execute($sql, $params = [])
    {
        //return parent::execute($sql, $params); // TODO: Change the autogenerated stub
        $con = self::getConnection();
        $sql = trim($sql);
        $mysql = $con->getPdo(stripos($sql, "SELECT") === 0 ? "read" : "write");
        $stmt = $mysql->prepare($sql);
        if($stmt !== false){
            return $stmt->execute($params);
        }else{
            return false;
        }
    }
}