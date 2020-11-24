<?php
namespace Inphp\DB;

use PDO;
use PDOException;
use Inphp\DB\Swoole\Pool;
use Swoole\Coroutine;

/**
 *
 * Class DB
 * @package Inphp\DB
 */
class DB
{

    public static $config = [];

    /**
     * 获取数据库配置
     * @return array
     */
    public static function getConfig(){
        if(!empty(self::$config)){
            return self::$config;
        }
        if(defined("DB_CONFIG")){
            self::$config = is_string(DB_CONFIG) ? (is_file(DB_CONFIG) ? require(DB_CONFIG) : []) : (is_array(DB_CONFIG) ? DB_CONFIG : []);
        }
        return self::$config;
    }

    /**
     * @param $table
     * @param null $as
     * @return Query|Swoole\Query
     */
    public static function from($table, $as = null)
    {
        $query = defined("DB_SWOOLE_POOLS") ? new \Inphp\DB\Swoole\Query() : new Query();
        return $query->from($table, $as);
    }

    /**
     * 记录日志，一般是记录错误信息
     * @param string $text 日志内容
     * @param bool $output 是否要输出给用户看
     */
    public static function log($text, $output=false){
        if(!empty($text)){
            $dir = defined('RUNTIME') ? RUNTIME."/db" : (defined('ROOT') ? ROOT."/runtime/db" : __DIR__."/runtime/db");
            if(!is_dir($dir)){
                if(!@mkdir($dir, 0777, true)){
                    echo "目录不可创建：{$dir}".PHP_EOL;
                }
            }
            $f = fopen($dir."/error.txt", "a");
            if($f){
                @fwrite($f, "时间：".date("Y/m/d H:i:s", time())."\r\n  ".$text."\r\n\r\n");
                @fclose($f);
            }
        }
        if($output || defined("DB_SWOOLE_POOLS")){
            $text = str_replace("\r\n", "<br>", $text);
            echo $text.PHP_EOL;
        }
    }

    /**
     * 数据库连接对象（静态对象，保证全局只使用一次连接）
     * @var Connection
     */
    private static $connection = null;

    /**
     * 连接池
     * @var \Inphp\DB\Swoole\Connection[]
     */
    private static $connections = [];

    /**
     * 获取数据库连接对象
     * @return Connection
     */
    public static function getConnection(){
        if(defined("DB_SWOOLE_POOLS")){
            $cid = Coroutine::getCid();
            if(isset(self::$connections[$cid]) && self::$connections[$cid] instanceof \Inphp\DB\Swoole\Connection){
                return self::$connections[$cid];
            }
            self::$connections[$cid] = Pool::getPool();
            //协程退出时会执行
            Coroutine::defer(function () use ($cid){
                unset(self::$connections[$cid]);
            });
            return self::$connections[$cid];
        }else{
            if(is_null(self::$connection)){
                self::$connection = new Connection(self::getConfig());
            }
            return self::$connection;
        }
    }

    /**
     * 事务开始
     */
    public static function begin()
    {
        self::getConnection()->begin();
    }

    /**
     * 事务回滚
     */
    public static function rollback()
    {
        self::getConnection()->rollback();
    }

    /**
     * 提交事务
     */
    public static function commit(){
        self::getConnection()->commit();
    }

    /**
     *
     * @param string $type
     * @return PDO
     */
    public static function getPdo($type = 'read'){
        return self::getConnection()->getPdo($type);
    }

    /**
     * 执行SQL语句
     * @param $sql
     * @param array $params
     * @return int
     */
    public static function execute($sql, $params = []){
        $con = self::getConnection();
        $sql = trim($sql);
        $pdo = $con->getPdo(stripos($sql, "SELECT") === 0 ? "read" : "write");
        if(defined("DB_SWOOLE_POOLS")){
            $stmt = $pdo->prepare($sql);
            if($stmt !== false){
                return $stmt->execute($params);
            }else{
                return false;
            }
        }else{
            try{
                $stm = $pdo->prepare($sql);
                if($stm->execute($params)){
                    return $stm->rowCount();
                }else{
                    $code = intval($stm->errorCode());
                    throw new PDOException($stm->errorInfo()[2]."<br>query : ".$stm->queryString."<br>code source : ".$code, intval($code));
                }
            }catch (PDOException $exception){
                $con->setError($exception->getCode(), $exception->errorInfo);
                return false;
            }
        }
    }

    /**
     * 获取表名前缀
     * @return mixed|string
     */
    public static function getTablePrefix(){
        $config = self::getConfig();
        return $config["prefix"] ?? "pre_";
    }
}