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
namespace Inphp\DB\Swoole;

class DB extends \Inphp\DB\DB
{

    /**
     * 查询开始
     * @param $tableName
     * @param null $as
     * @return \Inphp\DB\Swoole\Query
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
        return Pool::getPool();
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