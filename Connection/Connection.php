<?php
/**
 * Created by PhpStorm.
 * Date: 2017/6/17
 * Time: PM6:03
 */
namespace DB\Connection{

    use DB\Cache\driver\Redis;
    use DB\DB;
    use PDO;
    use PDOException;

    class Connection{

        public $success = false;
        public $pdo = [];
        public $config = [];
        public $database = [];
        public $lastInsertId;
        public $prefix = [];
        public $driver;
        public $error;
        public $errorId = 0;
        public $rowCount = 0;
        public $inTransaction = false;
        public $errorDisplay = [];
        public $redis = null;

        public $autoRelease = false;

        public function __construct(array $config){
            //读
            $read_config = isset($config['read']) ? $config['read'] : [];
            $read_config = !empty($read_config) ? $read_config : (isset($config['default']) ? $config['default'] : []);
            $read_config = !empty($read_config) ? $read_config : (!empty($config) ? $config : null);
            //写
            $write_config = isset($config['write']) ? $config['write'] : [];
            $write_config = !empty($write_config) ? $write_config : (isset($config['default']) ? $config['default'] : []);
            $write_config = !empty($write_config) ? $write_config : (!empty($config) ? $config : null);
            if($read_config===null || $read_config===null){
                DB::log("数据库连接配置未设置", true);
            }
            //连接数据库
            $this->connect($read_config, 'read');
            $this->connect($write_config, 'write');
            //尝试连接redis
            if (extension_loaded('redis')) {
                //尝试在缓存中获取，默认使用 redis 扩展
                if(isset($config['redis'])){
                    $redis = new Redis($config['redis']);
                    if(!is_null($redis)){
                        $this->redis = $redis;
                    }
                }
            }
        }

        public function connect(array $config=[], $type){
            $this->config[$type] = $config;
            $this->driver = isset($config["driver"]) ? $config["driver"] : "mysql";
            $this->database[$type] = isset($config["database"]) ? $config["database"] : "z_db";
            $this->prefix[$type] = isset($config["prefix"]) ? $config["prefix"] : "db_";
            $this->errorDisplay[$type] = isset($config["error_display"]) ? $config["error_display"] : false;
            $username = isset($config["user"]) ? $config["user"] : $this->database[$type];
            $password = isset($config["pass"]) ? $config["pass"] : "";
            $host = isset($config["host"]) ? $config["host"] : "127.0.0.1";
            $port = isset($config["port"]) ? $config["port"] : "3306";
            $charset = isset($config["charset"]) ? $config["charset"] : "utf8";
            //暂时只测试了Mysql数据库引擎
            switch($this->driver){
                case "mssql" :
                    $dns = "odbc:Driver={SQL Server};Server={$host},{$port};Database={$this->database[$type]};";
                    break;
                case "sqlite" :
                    $dns = "sqlite:{$this->database[$type]}";
                    break;
                case "oracle" :
                    $dns = "oci:dbname={$this->database[$type]}";
                    break;
                case "mysql" :
                    $dns = "mysql:host={$host};port={$port};dbname={$this->database[$type]}";
                    break;
                default :
                    throw new PDOException("error DNS", 38);
                    break;
            }
            //开始连接数据库
            try{
                $this->pdo[$type] = new PDO($dns, $username, $password,
                    array(
                        PDO::ATTR_PERSISTENT => 1,
                        PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES '{$charset}';",
                        PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true
                    )
                );
                //在默认情况下，PDO并没有让MySQL数据库执行真正的预处理语句。为了解决这个问题，要禁止模拟预处理语句！
                $this->pdo[$type]->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);//允许PDO模拟预处理语句
                $this->pdo[$type]->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);//这个不是必须的，但是建议加上，这样在脚本出错时，不会停止运行，而是会抛出异常！
                $this->success = true;
            }catch (PDOException $e){
                $this->connectFail($e, $type);
            }
        }

        private function connectFail(PDOException $e, $type){
            DB::log($e->getMessage(), $this->errorDisplay[$type]);
        }

        /**
         * @param string $type
         * @return PDO
         */
        public function getPdo($type='read'){
            return $this->pdo[$type];
        }

        /**
         * 重新连接数据库
         */
        public function reConnect(){
            DB::log("正在重启Mysql连接！");
            $this->connect($this->config['read'], 'read');
            $this->connect($this->config['write'], 'write');
        }

        /**
         * 开始事务，这一个事务开始后，需要手动提交或者回滚，否则Query里边的事务不会开始，也不会提交，更不会有回滚
         */
        public function beginTransaction(){
            if(!$this->inTransaction){
                $this->pdo["write"]->beginTransaction();
                $this->inTransaction = true;
            }
        }

        /**
         * 事务提交
         */
        public function commit(){
            if($this->inTransaction){
                $this->pdo["write"]->commit();
            }
            $this->inTransaction = false;
        }

        /**
         * 事务回滚
         */
        public function rollBack(){
            if($this->inTransaction){
                $this->pdo["write"]->rollBack();
            }
            $this->inTransaction = false;
        }

        /**
         * 设置错误信息
         * @param $message
         * @param $code
         */
        public function setError($message, $code){
            $this->error = $message;
            $this->errorId = $code;
        }

        /**
         * 自动释放连接，主要用于服务端运行时使用的
         */
        public function autoReleasePdo(){
            if($this->autoRelease){
                $this->pdo["write"] = null;
                $this->pdo["read"] = null;
            }
        }

    }
}