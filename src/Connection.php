<?php
/**
 * Create By Hunter
 * 2019-07-29 01:20
 */
namespace Inphp\DB;

use PDO;
use PDOException;

/**
 * 数据库连接类
 * Class Connection
 * @package Inphp\DB
 */
class Connection {

    /**
     * 连接配置
     * @var array
     */
    public $configs = [
        'read'  => null,
        'write' => null
    ];

    /**
     * @var [PDO]
     */
    public $pdo = [
        'read'  => null,
        'write' => null
    ];

    /**
     * 是否在错误时显示错误信息
     * @var bool
     */
    public $debug = false;

    /**
     * 表名前缀
     * @var string
     */
    public $prefix = "";

    /**
     * 是否正在处理事务中
     * @var bool
     */
    public $inTransaction = false;

    /**
     * 错误记录
     * @var array
     */
    private $error = [
        'error' => null,
        'id'    => null
    ];

    /**
     * 遍历模式
     * @var int
     */
    public $fetchModel = PDO::FETCH_ASSOC;

    /**
     * 超时时间
     * @var int
     */
    public $timeout = 5;

    /**
     * 初始化数据库连接，需要指定配置
     * Connection constructor.
     * @param array $config
     */
    public function __construct(array $config = null)
    {
        if(!is_null($config)){
            //读
            $read = $config['read'] ?? [];
            $read = !empty($read) ? $read : ($config['default'] ?? []);
            $read = !empty($read) ? $read : $config;
            //写
            $write = $config['write'] ?? [];
            $write = !empty($write) ? $write : ($config['default'] ?? []);
            $write = !empty($write) ? $write : $config;
            if(empty($read) || empty($write)){
                //未定义任何连接
                echo "未定义任何连接".PHP_EOL;
            }
            //是否打开调试
            if(isset($config['debug'])){
                $this->debug = $config['debug'] == true;
            }
            //遍历模式
            if(isset($config['fetch_model'])){
                $this->fetchModel = $config['fetch_model'];
            }
            //表名前缀
            if(isset($config['prefix'])){
                $this->prefix = $config['prefix'];
            }
            //超时
            if(isset($config['timeout'])){
                $this->timeout = $config['timeout'];
            }
            //保存到类属性
            $this->configs = [
                'read' => $read,
                'write'=> $write
            ];
        }
    }

    /**
     * 另类的初始化
     * @param null $config
     * @param string $type
     * @return Connection
     */
    public function init($config = null, $type = 'read')
    {
        $this->configs[$type] = $config ?? $this->configs[$type];
        return $this;
    }

    /**
     * @param null $config
     * @param string $type
     * @return false|PDO
     */
    public function connect($config = null, $type = 'read')
    {
        if(isset($this->pdo[$type]) && $this->pdo[$type] instanceof PDO){
            return $this->pdo[$type];
        }
        $this->configs[$type] = $config ?? $this->configs[$type];
        $config = $this->configs[$type];
        //如果存在其中一个
        if(($type == 'read' && isset($this->pdo['write']) && $this->pdo['write'] instanceof PDO) || ($type == 'write' && isset($this->pdo['read']) && $this->pdo['read'] instanceof PDO)){
            if($config == $this->configs[$type == 'read' ? 'write' : 'read']){
                //配置相同
                $this->pdo[$type] = $this->pdo[$type == 'read' ? 'write' : 'read'];
                return $this->pdo[$type];
            }
        }
        //取参数
        $host = $config['host'] ?? '127.0.0.1';
        $port = $config['port'] ?? 3306;
        $user = $config['user'] ?? ($config['username'] ?? 'root');
        $pass = $config['pass'] ?? ($config['password'] ?? '');
        $database = $config['database'] ?? ($config['dbname'] ?? 'database');
        $charset = $config['charset'] ?? 'utf8';
        $timeout = $config['timeout'] ?? $this->timeout;
        $timeout = is_numeric($timeout) && $timeout>=0 ? ceil($timeout) : 5;
        //连接mysql数据库
        $dns = "mysql:host={$host};port={$port};dbname={$database}";
        try {
            $this->pdo[$type] = new PDO($dns, $user, $pass, [
                PDO::ATTR_PERSISTENT => 1,
                PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES '{$charset}';",
                PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true,
                PDO::ATTR_EMULATE_PREPARES => true,
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_TIMEOUT => $timeout
            ]);
            return $this->pdo[$type];
        }catch (PDOException $exception) {
            //连接数据库失败
            echo "连接数据库失败".PHP_EOL;
            return false;
        }
    }

    /**
     * 记录重连接次数
     * @var int
     */
    private $reconnectionTimes = 0;

    /**
     * 重连接
     * @param string $type
     * @return bool|false|PDO
     */
    public function reconnect($type = 'read')
    {
        if($this->reconnectionTimes < 10){
            return $this->connect(null, $type);
        }
        return false;
    }

    /**
     * @param string $type
     * @return false|PDO
     */
    public function getPdo($type = 'read')
    {
        return $this->pdo[$type] ?? $this->connect(null, $type);
    }

    /**
     * 获取表前缀
     * @param string $type
     * @return string
     */
    public function getPrefix($type = 'read')
    {
        $config = $this->configs[$type] ?? [];
        return $config['prefix'] ?? DB::getTablePrefix();
    }

    /**
     * 获取数据库名
     * @param string $type
     * @return string
     */
    public function getDBName($type = 'read')
    {
        $config = $this->configs[$type] ?? [];
        return $config['database'] ?? ($config['dbname'] ?? '');
    }

    /**
     * 开始事务，所使用的连接肯定是 write
     * @return false|PDO
     */
    public function begin()
    {
        $pdo = $this->getPdo('write');
        if(!$this->inTransaction)
        {
            $pdo->beginTransaction();
        }
        $this->inTransaction = true;
        return $pdo;
    }

    /**
     * 事务回滚
     */
    public function rollback()
    {
        if($this->inTransaction)
        {
            $pdo = $this->getPdo('write');
            $pdo->rollBack();
        }
        $this->inTransaction = false;
    }

    /**
     * 提交事务
     */
    public function commit()
    {
        if($this->inTransaction)
        {
            $pdo = $this->getPdo('write');
            $pdo->commit();
        }
        $this->inTransaction = false;
    }

    /**
     * 保存错误
     * @param $id
     * @param $error
     */
    public function setError($id, $error)
    {
        $this->error = [
            'error' => $error,
            'id'    => $id
        ];
        DB::log("Error ID : {$id}\r\nError : {$error}", $this->debug);
    }

    /**
     * 获取错误
     * @return string
     */
    public function getError()
    {
        return $this->error['error'];
    }
}