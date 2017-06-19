<?php
/**
 * Created by PhpStorm.
 * Date: 2017/6/17
 * Time: PM5:17
 */
namespace DB{

    use DB\Connection\Connection;
    use DB\Query\QueryBuilder;

    class DB{
        /**
         * 连接池
         * @var array
         */
        public static $connections = [];

        /**
         * 数据库连接初始化
         * @param array $config
         * @param string $name
         */
        public static function init(array $config, $name='default'){
            $connection = new Connection($config);
            if($connection->success)
                self::$connections[$name] = $connection;
            else
                self::log($connection->error, true);
        }


        /**
         * 获取数据表前缀
         * @param string $name 链接名称
         * @param string $type 读写分离的话需要用到，不过一般读写分离的话，数据表的前缀一般都是一样的。
         * @return string
         */
        public static function getTablePrefix($name='default', $type='read'){
            $connection = self::getConnection($name);
            return $connection->prefix[$type];
        }

        /**
         * 获取数据库名称
         * @param string $name 链接名称
         * @param string $type 读写分离的话需要用到
         * @return mixed
         */
        public static function getDBName($name='default', $type='read'){
            $connection = self::getConnection($name);
            return $connection->database[$type];
        }
        /**
         * @param string $table 需要操作的表名称
         * @param string $name 链接名称
         * @return QueryBuilder
         */
        public static function from($table, $name='default'){
            $connection = self::getConnection($name);
            $query = new QueryBuilder($connection);
            return $query->from($table);
        }
        /**
         * @param $table
         * @return QueryBuilder
         */
        public static function table($table){
            return self::from($table);
        }

        /**
         * 查询开始 DB::select('*')->from(...)-> ...
         * @param array $columns
         * @param string $name
         * @return QueryBuilder
         */
        public static function select($columns=['*'], $name='default'){
            $connection = self::getConnection($name);
            $query = new QueryBuilder($connection);
            return $query->select($columns);
        }

        /**
         * 开始事务
         * @param string $name
         */
        public static function beginTransaction($name='default'){
            $connection = self::getConnection($name);
            $connection->beginTransaction();
        }

        /**
         * 事务回滚
         * @param string $name
         */
        public static function rollBack($name='default'){
            $connection = self::getConnection($name);
            $connection->rollBack();
        }

        /**
         * 提交事务
         * @param string $name
         */
        public static function commit($name='default'){
            $connection = self::getConnection($name);
            $connection->commit();
        }

        /**
         * 获取连接所用的PDO
         * @param string $name
         * @param string $type
         * @return PDO
         */
        public static function getPdo($name='default', $type='read'){
            $connection = self::getConnection($name);
            return $connection->getPdo($type);
        }

        /**
         * 获取错误信息，由code和info组合。
         * @param string $name
         * @return string
         */
        public static function getError($name='default'){
            $connection = self::getConnection($name);
            return $connection->error;
        }

        /**
         * 获取上一次执行{insert,update,delete}所影响的行数
         * 注意：即使执行SQL成功，若字段值相比之前的没有任何改变，那么影响行数＝0，仅在数据发生改变时，才会大于0
         * @param string $name
         * @return int
         */
        public static function rowCount($name='default'){
            $connection = self::getConnection($name);
            return $connection->rowCount;
        }

        /**
         * 直接执行sql语句返回结果
         * @param $sql
         * @param int $model
         * @param string $name
         * @return void|array
         */
        public static function get($sql, $model=PDO::FETCH_ASSOC, $name='default'){
            $pdo = self::getPdo($name);
            if($result = $pdo->query($sql)){
                $result->setFetchMode($model);
                return $result->fetchAll();
            }else{
                echo "code : ".$pdo->errorCode()."<br>error : ".$pdo->errorInfo()[2]."<br>query : ".$sql;
                exit;
            }
        }

        /**
         * 获取一条记录
         * @param $sql
         * @param $model
         * @param string $name
         * @return array|void
         */
        public static function getOne($sql, $model=PDO::FETCH_ASSOC, $name='default'){
            $pdo = self::getPdo($name);
            if($result = $pdo->query($sql)){
                $result->setFetchMode($model);
                return $result->fetch();
            }else{
                echo "code : ".$pdo->errorCode()."<br>error : ".$pdo->errorInfo()[2]."<br>query : ".$sql;
                exit;
            }
        }

        /**
         * 获取字段列表
         * @param $sql
         * @param $field
         * @param string $name
         * @return array
         */
        public static function lists($sql, $field, $name='default'){
            if($data = self::get($sql, PDO::FETCH_ASSOC, $name)){
                $list = array();
                foreach($data as $d){
                    if(isset($d[$field])){
                        $list[] = $d[$field];
                    }
                }
                return $list;
            }else{
                return array();
            }
        }

        /**
         * 获取1条记录
         * @param $sql
         * @param string $name
         * @return array|void
         */
        public static function first($sql, $name='default'){
            $sql .= " limit 0,1";
            $pdo = self::getPdo($name);
            if($result = $pdo->query($sql)){
                $result->setFetchMode(PDO::FETCH_ASSOC);
                $row = $result->fetchAll();
                return !empty($row) ? end($row) : array();
            }else{
                echo "code : ".$pdo->errorCode()."<br>error : ".$pdo->errorInfo()[2]."<br>query : ".$sql;
                exit;
            }
        }

        /**
         * 执行sql语句
         * @param $sql
         * @param string $name
         * @param string $type
         * @return bool
         * @return array|void
         */
        public static function execute($sql, $name='default', $type='write'){
            $pdo = self::getPdo($name, $type);
            $connection = self::getConnection($name);
            $rows = $pdo->exec($sql);
            $rows = $rows===0 ? true : $rows;
            if($rows){
                $connection->error = '';
                return $rows;
            }else{
                $connection->error = "code : ".$pdo->errorCode()."<br>error : ".$pdo->errorInfo()[2]."<br>query : ".$sql;
                return false;
            }
        }

        /**
         * 获取连接
         * @param string $name
         * @return Connection|void
         */
        public static function getConnection($name='default'){
            $connection = isset(self::$connections[$name]) ? self::$connections[$name] : false;
            if(!$connection){
                exit('undefined connection');
            }
            return $connection;
        }

        /**
         * 销毁连接
         * @param string $name
         */
        public static function destroy($name='default'){
            if(isset(self::$connections[$name])){
                unset(self::$connections[$name]);
            }
        }

        /**
         * 获取版本信息
         * @param string $name
         * @return mixed
         */
        public static function getVersion($name='default'){
            $result = self::first('select VERSION() version', $name);
            return $result['version'];
        }

        /**
         * 获取表的字段列表
         * @param $tableName
         * @param string $name
         * @return array
         */
        public static function getFieldsListFromTable($tableName, $name='default'){
            return self::from('INFORMATION_SCHEMA.COLUMNS')->where('TABLE_SCHEMA', self::getDBName($name))->where('TABLE_NAME', self::getTablePrefix().$tableName)->lists('COLUMN_NAME');
        }

        /**
         * 记录日志，一般是记录错误信息
         * @param string $text 日志内容
         * @param bool $output 是否要输出给用户看
         */
        public static function log($text, $output=false){
            if(!empty($text)){
                $f = fopen(__DIR__."/Log/error.txt", "a");
                if($f){
                    @fwrite($f, $text);
                    @fclose($f);
                }
            }
            if($output){
                exit("<font style='font-size:12px;'>{$text}</font>");
            }
        }

    }
}