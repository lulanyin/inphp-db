<?php
/**
 * Created by PhpStorm.
 * Date: 2017/6/17
 * Time: PM5:59
 */
namespace DB\Query{

    use DB\Connection\Connection;
    use DB\DB;
    use DB\Grammar\GrammarBuilder;
    use PDO;

    class QueryBuilder{
        /**
         * @var GrammarBuilder
         */
        public $grammar;
        /**
         * @var PDO
         */
        public $pdo;
        public $fetchModel      = PDO::FETCH_ASSOC;     //遍历方式
        public $lastInsertId    = [];                   //执行产生的自增ID
        public $affectRows      = 0;                    //所影响的行数
        public $queryString     = "";                   //生成的查询语句
        /**
         * @var Connection;
         */
        public $connection;
        public $tableName       = '';                   //需要查询的表
        public $columns         = ["*"];                //需要查询的列名
        public $wheres          = [];                   //查询条件
        public $whereParamValues= [];                   //查询条件参数化的对应值["key"=>"value"] => [:key, value]
        public $joins           = [];                   //关联条件
        public $joinAs          = "";                   //关联时的别名
        public $groupBy         = [];                   //分组条件
        public $orderBy         = [];                   //排序条件
        public $having          = [];                   //侧重条件
        public $unions          = [];                   //联合查询
        public $set             = [];                   //插入、更新操作的时候，所包含的字段对应字段值

        public $error           = "";                   //错误信息
        public $errorId         = 0;                    //错误ID

        public $result          = [];                   //查询结果

        public $operators       = [
            '=', '>', '>=', '<', '<=', '!=', '<>',
            'like', 'not like', 'like binary',
            'in', 'not in',
            'between', 'not between',
            'is null', 'is not null',
            'regexp', 'not regexp',
            'find_in_set', 'not find_in_set'
        ];                                              //对比符号

        /**
         * QueryBuilder constructor.
         * @param Connection $connection
         * @return QueryBuilder
         */
        public function __construct(Connection $connection){
            $this->connection = $connection;
            switch ($connection->driver){
                case "mssql" :

                    break;
                case "sqlite" :

                    break;
                case "oracle" :

                    break;
                default :
                    $this->grammar = new GrammarBuilder($this);
                    break;
            }
            return $this;
        }

        /**
         * 启用一个新的 QueryBuilder
         * @return QueryBuilder
         */
        protected function newQuery(){
            return new QueryBuilder($this->connection);
        }

        /**
         * 开始查询
         * @param $tableName
         * @return QueryBuilder
         */
        public function from($tableName){
            $this->tableName = $tableName;
            return $this;
        }

        /**
         * 开始查询
         * @param $tableName
         * @return QueryBuilder
         */
        public function table($tableName){
            return $this->from($tableName);
        }

        /**
         * select [columns] from ....
         * @param array $columns
         * @return QueryBuilder
         */
        public function select($columns=['*']){
            $columns = is_array($columns) ? $columns : func_get_args();
            $list = [];
            foreach ($columns as $c){
                $c = preg_replace("/\s+/", "", $c);
                $c = str_replace("、", ",", $c);
                $c = str_replace("，", ",", $c);
                $c = str_replace("|", ",", $c);
                $list[] = $c;
            }
            $this->columns = $list;
            return $this;
        }

        /**
         * add select fields
         * @param array $columns
         * @return QueryBuilder
         */
        public function addSelect($columns=[]){
            $columns = is_array($columns) ? $columns : func_get_args();
            $list = [];
            foreach ($columns as $c){
                $c = preg_replace("/\s+/", "", $c);
                $c = str_replace("、", ",", $c);
                $c = str_replace("，", ",", $c);
                $c = str_replace("|", ",", $c);
                $list[] = $c;
            }
            $this->columns = array_merge($this->columns, $list);
            return $this;
        }

        /**
         * @param QueryBuilder|\Closure|string $tableName
         * @param array $on 关联条件
         * @param string $type
         * @return QueryBuilder
         */
        public function join($tableName, $on, $type="inner"){
            $status = "default";
            $as = "";
            if($tableName instanceof QueryBuilder){
                //->join(DB::from(''))...
                $queryString = $tableName->compileToQueryString();
                $this->whereParamValues = array_merge($this->whereParamValues, $tableName->whereParamValues);
                $as = $tableName->joinAs;
                if(empty($as)){
                    DB::log("关联查询时时，缺少别名声明", $this->connection->errorDisplay['read']);
                    DB::log("\r\n对应部分SQL语句：{$queryString}");
                }
                $table = "({$queryString}) {$as}";
                $status = "nested";
            }elseif($tableName instanceof \Closure){
                //->join(function($newQuery){ ... })
                $newQuery = $this->newQuery();
                call_user_func($tableName, $newQuery);
                $queryString = $newQuery->compileToQueryString();
                $this->whereParamValues = array_merge($this->whereParamValues, $newQuery->whereParamValues);
                $as = $newQuery->joinAs;
                if(empty($as)){
                    DB::log("关联查询时时，缺少别名声明", $this->connection->errorDisplay['read']);
                    DB::log("\r\n对应部分SQL语句：{$queryString}");
                }
                $table = "({$queryString}) {$as}";
                $status = "nested";
            }else{
                $table = $tableName;
            }
            $this->joins[] = compact("table", "as", "on", "type", "status");
            return $this;
        }
        public function innerJoin($tableName, $on){
            return $this->join($tableName, $on);
        }
        public function leftJoin($tableName, $on){
            return $this->join($tableName, $on, "left");
        }
        public function rightJoin($tableName, $on){
            return $this->join($tableName, $on, "right");
        }

        /**
         * 联合查询时，需要声明临时表的别名
         * @param string $as
         * @return QueryBuilder
         */
        public function joinAs($as){
            $this->joinAs = $as;
            return $this;
        }

        public function where($column, $operator=null, $value=null, $boolean='and'){
            if(is_array($column)){
                $column = is_array(end($column)) ? $column : [$column];
                foreach ($column as $c){
                    $len = count($c);
                    if($len==2){
                        $c[2] = $c[1];
                        $c[1] = "=";
                        $c[3] = "and";
                    }elseif($len==3){
                        $c[3] = "and";
                    }elseif($len==1){
                        $c[1] = $c[2] = null;
                        $c[3] = "and";
                    }
                    @$this->where($c[0], $c[1], $c[2], $c[3]);
                }
                return $this;
            }

        }
    }
}