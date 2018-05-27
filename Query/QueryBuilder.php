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
    use DB\Grammar\mssqlGrammar;
    use PDO;
    use Closure;

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
        public $result_query    = '';                   //查询结果对应的SQL语句
        public $result_query_params = [];                   //查询结果对应的SQL语句的参数值

        public $operators       = [
            '=', '>', '>=', '<', '<=', '!=', '<>',
            'like', 'not like', 'like binary',
            'in', 'not in',
            'between', 'not between',
            'is null', 'is not null',
            'regexp', 'not regexp',
            'find_in_set', 'not find_in_set'
        ];                                              //对比符号

        public $cache = false;                          //此次查询结果是否缓存
        public $cacheExpireTime = 0;                    //如果缓存，请设置缓存的时间（秒），默认值0，永久缓存，直到缓存功能失效
        public $cacheReload = false;                    //是否更新缓存

        /**
         * QueryBuilder constructor.
         * @param Connection $connection
         * @return QueryBuilder
         */
        public function __construct(Connection $connection){
            $this->connection = $connection;
            if(is_null($connection)){
                return $this;
            }
            switch ($connection->driver){
                case "mssql" :
                    $this->grammar = new mssqlGrammar($this);
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
        public function newQuery(){
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
                $c = preg_replace("/\s+/", " ", $c);
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
                //$c = preg_replace("/\s+/", "", $c);
                $c = str_replace("、", ",", $c);
                $c = str_replace("，", ",", $c);
                $c = str_replace("|", ",", $c);
                $list[] = $c;
            }
            $this->columns = array_merge($this->columns, $list);
            return $this;
        }

        /**
         * @param QueryBuilder|Closure|string $tableName
         * @param array $on 关联条件
         * @param string $type
         * @return QueryBuilder
         */
        public function join($tableName, $on, $type="inner"){
            $status = "default";
            $as = "";
            if($tableName instanceof QueryBuilder){
                //->join(DB::from(''))...
                list($queryString, $params) = $tableName->compileToQueryString();
                $this->grammar->flushParams($params);
                $as = $tableName->joinAs;
                if(empty($as)){
                    DB::log("关联查询时时，缺少别名声明", $this->connection->errorDisplay['read']);
                    DB::log("\r\n对应部分SQL语句：{$queryString}");
                }
                $table = "({$queryString}) {$as}";
                $status = "nested";
            }elseif($tableName instanceof Closure){
                //->join(function($newQuery){ ... })
                $newQuery = $this->newQuery();
                call_user_func($tableName, $newQuery);
                list($queryString, $params) = $newQuery->compileToQueryString();
                $this->grammar->flushParams($params);
                $as = $newQuery->joinAs;
                if(empty($as)){
                    DB::log("关联查询时时，缺少别名声明", $this->connection->errorDisplay['read']);
                    DB::log("\r\n对应部分SQL语句：{$queryString}", $this->connection->errorDisplay['read']);
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
            if(func_num_args()==2){
                list($operator, $value) = ['=', $operator];
            }elseif($this->invalidOperatorAndValue($operator, $value)){
                DB::log("所输入的对比符号不正确", $this->connection->errorDisplay['read']);
            }

            if($column instanceof Closure){
                return $this->whereNested($column, $boolean);
            }

            if (! in_array(strtolower($operator), $this->operators, true)) {
                list($value, $operator) = [$operator, '='];
            }

            if($value instanceof Closure){
                return $this->whereSub($column, $operator, $value, $boolean);
            }

            if (is_null($value)) {
                return $this->whereNull($column, $boolean, $operator != '=');
            }

            $type = 'basic';
            $this->wheres[] = compact('type', 'column', 'operator', 'value', 'boolean');
            return $this;
        }

        /**
         * or where 条件
         * @param $column
         * @param null $operator
         * @param null $value
         * @return QueryBuilder
         */
        public function orWhere($column, $operator=null, $value=null){
            if(func_num_args()==2){
                list($value, $operator) = array($operator, "=");
            }
            return $this->where($column, $operator, $value, 'or');
        }

        /**
         * @param $sql
         * @param string $boolean
         * @return $this
         */
        public function whereRaw($sql, $boolean='and'){
            $type = 'Raw';
            $this->wheres[] = compact('type', 'sql', 'boolean');
            return $this;
        }
        public function orWhereRaw($sql){
            return $this->whereRaw($sql, 'or');
        }

        /**
         * 添加一个子查询
         * @param $column
         * @param $operator
         * @param Closure $callback
         * @param $boolean
         * @return QueryBuilder
         */
        public function whereSub($column, $operator, Closure $callback, $boolean='and'){
            $type = 'Sub';
            $query = $this->newQuery();
            call_user_func($callback, $query);
            $this->wheres[] = compact('type', 'column', 'operator', 'query', 'boolean');
            return $this;
        }

        /**
         * 嵌套查询
         * @param Closure $callBack
         * @param string $boolean
         * @return QueryBuilder
         */
        public function whereNested(Closure $callBack, $boolean='and'){
            $query = $this->newQuery();
            $query->from($this->tableName);
            call_user_func($callBack, $query);
            return $this->addNestedWhereQuery($query, $boolean);
        }

        /**
         *
         * @param $query
         * @param string $boolean
         * @return QueryBuilder
         */
        public function addNestedWhereQuery($query, $boolean='and'){
            if (count($query->wheres)) {
                $type = 'Nested';
                $this->wheres[] = compact('type', 'query', 'boolean');
            }
            return $this;
        }

        /**
         * 字段值是否为 null 的条件
         * @param $column
         * @param string $boolean
         * @param bool $not
         * @return QueryBuilder
         */
        public function whereNull($column, $boolean='and', $not=false){
            $type = $not ? 'NotNull' : 'Null';
            $this->wheres[] = compact('type', 'column', 'boolean');
            return $this;
        }
        public function orWhereNull($column, $not=false){
            return $this->whereNull($column, 'or', $not);
        }
        public function whereNotNull($column, $boolean='and'){
            return $this->whereNull($column, $boolean, true);
        }
        public function orWhereNotNull($column){
            return $this->orWhereNull($column, true);
        }

        /**
         * 字段值是否为空的条件
         * @param $column
         * @param string $boolean
         * @param bool $not
         * @return QueryBuilder
         */
        public function whereEmpty($column, $boolean='and', $not=false){
            return $this->where(function($eq) use($column, $not, $boolean){
                $eq->where($column, $not ? '!=' : '=', '', $boolean);
                if($not){
                    $eq->whereNull($column, 'and', true);
                }else{
                    $eq->orWhereNull($column, false);
                }
            }, null, null, $boolean);
        }
        public function orWhereEmpty($column, $not=false){
            return $this->whereEmpty($column, 'or', $not);
        }
        public function whereNotEmpty($column, $boolean='and'){
            return $this->whereEmpty($column, $boolean, true);
        }
        public function orWhereNotEmpty($column){
            return $this->orWhereEmpty($column, true);
        }

        /**
         * @param $column
         * @param $value
         * @param string $boolean
         * @param bool $not
         * @return QueryBuilder
         */
        public function whereIn($column, $value, $boolean='and', $not=false){
            if($value instanceof Closure){
                return $this->whereInSub($column, $value, $boolean, $not);
            }
            $type = $not ? 'NotIn' : 'In';
            $this->wheres[] = compact('type', 'column', 'value', 'boolean');
            return $this;
        }

        /**
         * in 条件
         * @param $column
         * @param Closure $callback
         * @param $boolean
         * @param $not
         * @return QueryBuilder
         */
        public function whereInSub($column, Closure $callback, $boolean='and', $not=false){
            $type = $not ? 'NotIn' : 'In';
            $query = $this->newQuery();
            call_user_func($callback, $query);
            $this->wheres[] = compact('type', 'column', 'query', 'boolean');
            return $this;
        }

        public function whereNotIn($column, $value, $boolean='and'){
            return $this->whereIn($column, $value, $boolean, true);
        }
        public function orWhereIn($column, $value, $not=false){
            return $this->whereIn($column, $value, 'or', $not);
        }
        public function orWhereNotIn($column, $value){
            return $this->orWhereIn($column, $value, true);
        }

        /**
         * between 条件
         * @param $column
         * @param $value
         * @param string $boolean
         * @param bool $not
         * @return QueryBuilder
         */
        public function whereBetween($column, $value, $boolean='and', $not=false){
            $type = $not ? 'NotBetween' : 'Between';
            $this->wheres[] = compact('type', 'column', 'value', 'boolean');
            return $this;
        }
        public function whereNotBetween($column, $value, $boolean='and'){
            return $this->whereBetween($column, $value, $boolean, true);
        }
        public function orWhereBetween($column, $value, $not=false){
            return $this->whereBetween($column, $value, 'or', $not);
        }
        public function orWhereNotBetween($column, $value){
            return $this->orWhereBetween($column, $value, true);
        }

        /**
         * exists条件
         * @param $value
         * @param string $boolean
         * @param bool $not
         * @return QueryBuilder
         */
        public function whereExists($value, $boolean='and', $not=false){
            if($value instanceof Closure){
                return $this->whereExistsSub($value, $boolean, $not);
            }else{
                return $this;
            }
        }
        public function whereNotExists($value, $boolean='and'){
            return $this->whereExists($value, $boolean, true);
        }
        public function orWhereExists($value, $not=false){
            return $this->whereExists($value, 'or', $not);
        }
        public function orWhereNotExists($value){
            return $this->orWhereExists($value, true);
        }
        public function whereExistsSub(Closure $callback, $boolean='and', $not=false){
            $type = $not ? 'NotExists' : 'Exists';
            $query = $this->newQuery();
            call_user_func($callback, $query);
            $this->wheres[] = compact('type', 'query', 'boolean');
            return $this;
        }

        /**
         * find_in_set
         * @param $value
         * @param $column
         * @param string $boolean
         * @return QueryBuilder
         */
        public function whereFindInSet($value, $column, $boolean='and'){
            $type = 'FindInSet';
            $this->wheres[] = compact('type', 'column', 'value', 'boolean');
            return $this;
        }
        public function orWhereFindInSet($value, $column){
            return $this->whereFindInSet($value, $column, 'or');
        }

        /**
         * like 查询条件
         * @param $column
         * @param $value
         * @param string $boolean
         * @param bool $not
         * @return QueryBuilder
         */
        public function whereLike($column, $value, $boolean='and', $not=false){
            $type = $not ? 'NotLike' : 'Like';
            $this->wheres[] = compact('type', 'column', 'value', 'boolean');
            return $this;
        }
        public function orWhereLike($column, $value, $not=false){
            return $this->whereLike($column, $value, "or", $not);
        }
        public function whereNotLike($column, $value){
            return $this->whereLike($column, $value, 'and', true);
        }
        public function orWhereNotLike($column, $value){
            return $this->orWhereLike($column, $value, true);
        }

        /**
         * 正则匹配
         * @param $column
         * @param $value
         * @param string $boolean
         * @param bool $not
         * @return QueryBuilder
         */
        public function whereRegexp($column, $value, $boolean='and', $not=false){
            $type = $not ? 'NotRegexp' : 'Regexp';
            $this->wheres[] = compact('type', 'column', 'value', 'boolean');
            return $this;
        }
        public function orWhereRegexp($column, $value, $not=false){
            return $this->whereRegexp($column, $value, "or", $not);
        }
        public function whereNotRegexp($column, $value){
            return $this->whereRegexp($column, $value, 'and', true);
        }
        public function orWhereNotRegexp($column, $value){
            return $this->orWhereRegexp($column, $value, true);
        }

        /**
         * union
         * @param QueryBuilder $query
         * @param bool $all
         * @return QueryBuilder
         */
        public function union(QueryBuilder $query, $all=false){
            $this->unions[] = compact('query', 'all');
            return $this;
        }
        public function unionAll(QueryBuilder $query){
            return $this->union($query);
        }

        /**
         * @param $column
         * @param $operator
         * @param null $value
         * @return QueryBuilder
         */
        public function having($column, $operator, $value=null){
            if(func_num_args()==2){
                list($value, $operator) = [$operator, '='];
            }
            $this->having[] = compact('column', 'operator', 'value');
            return $this;
        }

        /**
         * group by
         * @param $columns
         * @return QueryBuilder
         */
        public function groupBy($columns){
            $this->groupBy[] = $columns;
            return $this;
        }

        /**
         * order by
         * @param $column
         * @param null $type
         * @return QueryBuilder
         */
        public function orderBy($column, $type=null){
            if(is_array($column)){
                foreach($column as $col){
                    if(!empty($col[0])){
                        $this->orderBy[] = array(
                            "column" => $col[0],
                            "type" => isset($col[1]) ? $col[1] : "ASC"
                        );
                    }
                }
            }else{
                $this->orderBy[] = is_array($column) ? $column : compact('column', 'type');
            }
            return $this;
        }

        /**
         * 解析成SQL语句
         * @param string $type
         * @return array
         */
        public function compileToQueryString($type='select'){
            return $this->grammar->compileToQueryString($type);
        }

        /**
         * 读取数据
         * @param int $total
         * @param int $offset
         * @return QueryBuilder|bool
         */
        public function get($total=null, $offset=0){
            $this->result = $this->grammar->get($total, $offset);
            $rs = $this->result!==false ? $this : false;
            if(!$rs){
                list($queryString, $params) = $this->compileToQueryString();
                DB::log("SQL 语法错误：\r\n{$queryString}\r\n参数：{".implode(",",$params)."}\r\n", $this->connection->errorDisplay['read']);
            }
            return $this->result;
        }
        public function getArray($total=null, $offset=0){
            return $this->get($total, $offset);
        }

        /**
         * 读取第一条数据
         * @return array
         */
        public function first(){
            $row = $this->get(1);
            if(is_array($row)){
                return is_array(end($row)) ? end($row) : [];
            }
           return [];
        }
        public function getOne(){
            return $this->first();
        }

        /**
         * 读取前X条
         * @param $total
         * @return QueryBuilder|bool
         */
        public function top($total){
            return $this->get($total);
        }

        public function count(){
            return $this->grammar->count();
        }
        public function rows(){
            return $this->count();
        }

        /**
         * @param $column
         * @param null $value
         * @param bool $valueIncludeField
         * @return QueryBuilder
         */
        public function set($column, $value=null, $valueIncludeField=false){
            if(is_array($column)){
                $columns = is_array(end($column)) ? $column : [$column];
                foreach($columns as $index=>$column){
                    foreach($column as $key=>$val){
                        $this->set[$index][] = [
                            'field' => $key,
                            'value' => $val,
                            'include_field' => $value===true || $value===false ? $value : $valueIncludeField
                        ];
                    }
                }
            }else{
                $this->set[0][] = [
                    'field' => $column,
                    'value' => $value,
                    'include_field' => $valueIncludeField
                ];
            }
            return $this;
        }

        /************************** 开始执行方法 **************************/

        /**
         * 执行更新数据库语句
         * @param null $columns
         * @return bool | QueryBuilder
         */
        public function update($columns=null){
            if(is_array($columns)){
                $this->set($columns);
            }
            if($this->grammar->update()){
                return $this;
            }
            return false;
        }

        /**
         * @param null $columns
         * @return bool | QueryBuilder
         */
        public function updateInsert($columns=null){
            $firstRow = $this->first();
            if(!empty($firstRow)){
                return $this->update($columns);
            }else{
                return $this->insert($columns);
            }
        }

        /**
         * 执行插入数据操作
         * @param null $set
         * @return bool | QueryBuilder
         */
        public function insert($set=null){
            if(is_array($set)){
                $this->set($set);
            }
            if($this->grammar->insert()){
                return $this;
            }
            return false;
        }

        /**
         * 当条件数据不存在时，插入数据
         * @return QueryBuilder|bool
         */
        public function insertIfNotExists(){
            if($this->grammar->insertIfNotExists()){
                return $this;
            }
            return false;
        }

        /**
         * 插入数据并返回自增ID，失败时返回0
         * @param null $set
         * @return array|int|mixed
         */
        public function insertGetId($set=null){
            if($this->insert($set)){
                return $this->getLastInsertId();
            }else{
                return 0;
            }
        }

        /**
         * 获取自增ID
         * @return array|int|mixed
         */
        public function getLastInsertId(){
            return count($this->lastInsertId)>1 ? $this->lastInsertId : (!empty($this->lastInsertId) ? end($this->lastInsertId) : 0);
        }

        /**
         * 删除数据
         * @return bool | QueryBuilder
         */
        public function delete(){
            if($this->grammar->delete()){
                return $this;
            }
            return false;
        }

        /**
         * 复制数据到表
         * @param $table
         * @param int $total
         * @param int $offset
         * @param array $unCopyFields
         * @param array $resetFields
         * @param $fn -- 复制完1条之后要执行的方法
         * @return bool|QueryBuilder
         */
        public function copyTo($table, array $unCopyFields=array(), array $resetFields=array(), $total=1, $offset=0, $fn=null){
            $data = $this->get($total, $offset)->toArray();
            if(!empty($data)){
                foreach($data as $key=>$d){
                    //删除不需要的字段，类似表的主键
                    foreach($unCopyFields as $uf){
                        if(isset($d[$uf])) unset($d[$uf]);
                    }
                    //重设、补加一些字段值，需要改变某些值字段的时候，很方便
                    foreach($resetFields as $field=>$value){
                        $d[$field] = $value;
                    }
                    $query = $this->newQuery()->from($table);
                    if($query->insert($d)){
                        $this->lastInsertId[] = $last_insert_id = $query->getLastInsertId();
                        if($fn instanceof Closure){
                            call_user_func($fn, $last_insert_id, $key);
                        }
                    }else{
                        $this->error = $query->error;
                        $this->errorId = $query->errorId;
                        return false;
                    }
                }
            }
            return $this;
        }

        /**
         * 获取影响的记录条数
         * @return int
         */
        public function getAffectRows(){
            return $this->affectRows;
        }

        /**
         * 获取错误
         * @return string
         */
        public function getError(){
            return $this->error;
        }

        /**
         * 从数据表中取得单一数据列的单一字段
         * @param string $column
         * @return string
         */
        public function pluck($column){
            $select = str_replace(".", "`.`", $column);
            $select = "`{$select}` pluck_field";
            $this->columns = [$select];
            $firstRow = $this->first();
            return isset($firstRow["pluck_field"]) ? $firstRow["pluck_field"] : "";
        }

        /**
         * 取得单一字段值的列表
         * @param string $column
         * @param bool $distinct
         * @return array
         */
        public function lists($column, $distinct=false){
            $this->columns = $distinct ? ["distinct {$column} as list_column"] : ["{$column} as list_column"];
            $rows = $this->get();
            $list = array();
            foreach($rows as $r){
                if(isset($r["list_column"])){
                    $list[] = $r["list_column"];
                }
            }
            return $list;
        }

        /**
         * 获取聚合函数统计的值
         * @param $column
         * @param $fn
         * @return string
         */
        protected function aggregateFunction($column, $fn){
            $string = $fn."({$column}) as val";
            $this->columns = [$string];
            $row = $this->first();
            //echo $this->sql;
            return isset($row["val"]) ? $row["val"] : "";
        }

        /**
         * 获取字段最大值
         * @param $column
         * @return string
         */
        public function max($column){
            return $this->aggregateFunction($column, "max");
        }

        /**
         * 获取字段的最小值
         * @param $column
         * @return string
         */
        public function min($column){
            return $this->aggregateFunction($column, "min");
        }

        /**
         * 获取平均值
         * @param $column
         * @param $round
         * @return string
         */
        public function avg($column, $round=2){
            $string = "round(avg(`{$column}`), {$round}) as val";
            $this->columns = [$string];
            $row = $this->first();
            return isset($row["val"]) ? $row["val"] : "";
        }

        /**
         * 获取总和
         * @param $column
         * @return string
         */
        public function sum($column){
            return $this->aggregateFunction($column, "sum");
        }

        /**
         * 快速清空数据表，并重置自增ID
         * @return bool|GrammarBuilder
         */
        public function truncate(){
            return $this->grammar->truncate();
        }

        /**
         * 检查条件对比符号是否合法
         * @param $operator
         * @param $value
         * @return bool
         */
        protected function invalidOperatorAndValue($operator, $value){
            $isOperator = in_array($operator, $this->operators);
            return $isOperator && $operator != '=' && is_null($value);
        }



        /**
         * 返回查询结果
         * @return array
         */
        public function toArray(){
            return $this->result;
        }

        /**
         * 将结果返回为JSON数据格式
         * @param int $int
         * @return string
         */
        public function toJson($int = JSON_UNESCAPED_UNICODE){
            return json_encode($this->result, $int);
        }
        public function getJson(){
            return $this->get()->toJson();
        }

        /**
         * 内置的结果数据遍历方法
         * @param Closure $callback
         */
        public function each(Closure $callback){
            foreach($this->result as $key=>$row){
                call_user_func($callback, $row, $key);
            }
        }

        /**
         * 原生SQL赋值
         * @param $column
         * @param $value
         * @param null $set
         * @return QueryBuilder
         */
        public function setRaw($column, $value, $set=null){
            if(is_array($set)){
                @$this->set($set);
            }
            return $this->set($column, $value, true);
        }
        /**
         * 自增一个字段，可以同时更新其它字段
         * @param $column
         * @param int $number
         * @param null $set
         * @return QueryBuilder
         */
        public function increment($column, $number=1, $set=null){
            if($number<0){return $this->decrement($column, abs($number), $set);}
            return $this->setRaw($column, "`{$column}`+{$number}", $set);
        }

        /**
         * 自减一个字段，可以同时更新其它字段
         * @param $column
         * @param int $number
         * @param null $set
         * @return QueryBuilder
         */
        public function decrement($column, $number=1, $set=null){
            return $this->setRaw($column, "`{$column}`-{$number}", $set);
        }

        /**
         * 转换为 datetime 可接受的格式
         * @param $time
         * @return false|mixed|string
         */
        private function translateToDatetime($time){
            if(is_numeric($time) && strlen($time)==10){
                return date("Y-m-d H:i:s", $time);
            }
            $time = preg_replace("/[年|月|\/]$/", "-", $time);
            $time = str_replace("日", " ", $time);
            $time = preg_replace("/[时|分]$/", ":", $time);
            $time = str_replace("秒", "", $time);
            return str_replace("--", "-", trim($time));
        }

        /**
         * datetime 字段专用的时间段查询，可以
         * @param $column
         * @param $start
         * @param $end
         * @param boolean $datetime 字段是否是datetime类型，如果不是，默认使用int 10位长度去处理
         * @return QueryBuilder
         */
        public function whereDateTimeBetween($column, $start, $end, $datetime=true){
            $start = $datetime ? $this->translateToDatetime($start) : $start;
            $end = $datetime ? $this->translateToDatetime($end) : $end;
            return $this->whereBetween($column, ["'{$start}'", "'{$end}'"]);
        }

        /**
         * 从什么时间开始
         * @param $column
         * @param $start
         * @param boolean $datetime 字段是否是datetime类型，如果不是，默认使用int 10位长度去处理
         * @return QueryBuilder
         */
        public function whereDateTimeStartAt($column, $start, $datetime=true){
            $start = $datetime ? $this->translateToDatetime($start) : $start;
            return $this->whereRaw("{$column}>='{$start}'");
        }

        /**
         * 到什么时间结束
         * @param $column
         * @param $end
         * @param boolean $datetime 字段是否是datetime类型，如果不是，默认使用int 10位长度去处理
         * @return QueryBuilder
         */
        public function whereDateTimeEndAt($column, $end, $datetime=true){
            $end = $datetime ? $this->translateToDatetime($end) : $end;
            return $this->whereRaw("{$column}<='{$end}'");
        }

        /**
         * 查询，过去N个月
         * @param $column
         * @param $number
         * @param boolean $datetime 字段是否是datetime类型，如果不是，默认使用int 10位长度去处理
         * @return QueryBuilder
         */
        public function whereMonthAfter($column, $number, $datetime=true){
            $number = intval($number);
            if($number>0){
                $column = $datetime ? $column : "from_unixtime({$column})";
                $this->whereRaw("{$column} between date_sub(now(), interval {$number} month) and now()");
            }
            return $this;
        }

        /**
         * 查询过去 N 个月之前的数据
         * @param $column
         * @param $number
         * @param boolean $datetime 字段是否是datetime类型，如果不是，默认使用int 10位长度去处理
         * @return QueryBuilder
         */
        public function whereMonthBefore($column, $number, $datetime=true){
            $number = intval($number);
            if($number>0){
                $column = $datetime ? $column : "from_unixtime({$column})";
                $this->whereRaw("{$column}<=date_sub(now(), interval {$number} month)");
            }
            return $this;
        }

        /**
         * 查询过去 N1 个月份到 N2 个月份之内的数据
         * @param $column
         * @param $month
         * @param boolean $datetime 字段是否是datetime类型，如果不是，默认使用int 10位长度去处理
         * @return QueryBuilder
         * ->whereMonthBetween("时间字段", [6, 12]) 查询过去 6~12个月份内的数据
         * ->whereMonthBetween("时间字段", 6) 查询过去 6 个月内的数据
         */
        public function whereMonthBetween($column, $month, $datetime=true){
            $month = is_array($month) ? $month : [intval($month)];
            $start = isset($month[1]) ? $month[1] : $month[0];
            $start = "date_sub(now(), interval {$start} month)";
            $end = isset($month[1]) ? "date_sub(now(), interval {$month[0]} month)" : "now()";
            $column = $datetime ? $column : "from_unixtime({$column})";
            $this->whereRaw("{$column} between {$start} and {$end}");
            return $this;
        }

        /**
         * 查询过去 N 年之前的数据
         * @param $column
         * @param $number
         * @param boolean $datetime 字段是否是datetime类型，如果不是，默认使用int 10位长度去处理
         * @return QueryBuilder
         */
        public function whereYearBefore($column, $number, $datetime=true){
            $number = intval($number);
            if($number>0){
                $column = $datetime ? $column : "from_unixtime({$column})";
                $this->whereRaw("{$column}<=date_sub(now(), interval {$number} year)");
            }
            return $this;
        }

        /**
         * 查询过去 N1 个年份到 N2 个年份之内的数据
         * @param $column
         * @param $year
         * @param boolean $datetime 字段是否是datetime类型，如果不是，默认使用int 10位长度去处理
         * @return QueryBuilder
         */
        public function whereYearBetween($column, $year, $datetime=true){
            $year = is_array($year) ? $year : [intval($year)];
            $start = isset($year[1]) ? $year[1] : $year[0];
            $start = "date_sub(now(), interval {$start} year)";
            $end = isset($year[1]) ? "date_sub(now(), interval {$year[0]} year)" : "now()";
            $column = $datetime ? $column : "from_unixtime({$column})";
            $this->whereRaw("{$column} between {$start} and {$end}");
            return $this;
        }

        /**
         * 查询N周之前的，默认为本周
         * @param $column
         * @param int $number
         * @param boolean $datetime 字段是否是datetime类型，如果不是，默认使用int 10位长度去处理
         * @return QueryBuilder
         */
        public function whereWeek($column, $number = 0, $datetime=true){
            $column = $datetime ? $column : "from_unixtime({$column})";
            return $this->whereRaw("YEARWEEK({$column}, 1) = YEARWEEK(NOW(), 1)-{$number}");
        }

        /**
         * 查询N天之前的数据，默认今天
         * @param $column
         * @param int $number
         * @param boolean $datetime 字段是否是datetime类型，如果不是，默认使用int 10位长度去处理
         * @return QueryBuilder
         */
        public function whereDay($column, $number=0, $datetime=true){
            $column = $datetime ? $column : "from_unixtime({$column})";
            return $this->whereRaw("TO_DAYS(NOW())-TO_DAYS({$column})<={$number}");
        }


        /**
         * 可以设置当前的查询是否缓存，如果缓存，则请设置缓存时间， 默认0，永久缓存
         * @param null $cacheReload
         * @param int $expireTime
         * @return QueryBuilder
         */
        public function cache($cacheReload=null, $expireTime = 0){
            $this->cacheReload = is_null($cacheReload) ? $this->cacheReload : $cacheReload;
            $this->cache = true;
            $this->cacheExpireTime = $expireTime;
            return $this;
        }

    }
}