<?php
/**
 * Create By Hunter
 * 2019-07-29 14:05:49
 */
namespace Small\DB;

use Closure;
use PDO;
use Small\DB\Model\ModelBase;

/**
 * sql 操作对象
 * Class Query
 * @package Small\DB
 */
class Query
{
    /**
     * 数据库连接对象，里边包含了PDO
     * @var Connection
     */
    public $connection          = null;

    /**
     * 语法处理对象
     * @var Grammar
     */
    public $grammar             = null;

    /**
     * 需要查询的表
     * @var string|array
     */
    public $tableName           = null;

    /**
     * 查询字段
     * @var array
     */
    public $columns             = ['*'];

    /**
     * 条件
     * @var array
     */
    public $wheres              = [];

    /**
     * 条件的预处理参数值
     * @var array
     */
    public $whereParamValues    = [];

    /**
     * 关联查询
     * @var array
     */
    public $joins               = [];

    /**
     * 关联查询时的临时表别名
     * @var string
     */
    public $joinAs              = "";

    /**
     * 分组条件
     * @var array
     */
    public $groupBy             = [];

    /**
     * 排序条件
     * @var array
     */
    public $orderBy             = [];

    /**
     * 侧重查询条件
     * @var array
     */
    public $having              = [];

    /**
     * 联合查询
     * @var array
     */
    public $unions              = [];

    /**
     * 插入、修改时，对应的字段=>值
     * @var array
     */
    public $set                 = [];

    /**
     * 是否已属于聚合查询
     * @var bool
     */
    public $isAggregateQuery    = false;

    /**
     * 对比符号限定
     * @var array
     */
    public $operators           = [
        '=', '>', '>=', '<', '<=', '!=', '<>',
        'like', 'not like', 'like binary',
        'in', 'not in',
        'between', 'not between',
        'is null', 'is not null',
        'regexp', 'not regexp',
        'find_in_set', 'not find_in_set'
    ];

    /**
     * 执行插入时，会保存自增ID
     * @var array
     */
    public $lastInsertId = [];

    /**
     * 影响行数
     * @var int
     */
    public $affectRows = 0;

    /**
     * 初始化 Query,可以指定Connection, 如果是事务关联，务必指定Connection
     * Query constructor.
     * @param Connection|null $connection
     */
    public function __construct(Connection $connection = null)
    {
        $this->connection = $connection;
        $this->grammar = new Grammar($this);
    }

    /**
     * 新建一个 Query 对象
     * @return Query
     */
    public function newQuery()
    {
        return new Query($this->connection);
    }

    /**
     * 开始查询 select from {table}
     * @param $table
     * @param null $as
     * @return Query
     */
    public function from($table, $as = null) : Query
    {
        if($table instanceof ModelBase){
            $m = new $table();
            $table = $m->getTableName().(!empty($as) ? " {$as}" : '');
            unset($m);
        }elseif($table instanceof Query){
            $table = [
                'query' => $table,
                'as'    => $as
            ];
        }
        $this->tableName = $table;
        return $this;
    }

    /**
     * 查询字段
     * @param array $columns
     * @return Query
     */
    public function select($columns = ['*']) : Query
    {
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
     * @return Query
     */
    public function addSelect($columns=[]) : Query
    {
        $columns = is_array($columns) ? $columns : func_get_args();
        $list = [];
        foreach ($columns as $c){
            $c = preg_replace("/\s+/", " ", $c);
            $c = str_replace("、", ",", $c);
            $c = str_replace("，", ",", $c);
            $c = str_replace("|", ",", $c);
            $list[] = $c;
        }
        $this->columns = array_merge($this->columns, $list);
        return $this;
    }

    /**
     * any_value(xxx)
     * @param $columns
     * @param null $as
     * @return Query
     */
    public function selectAnyValue($columns, $as=null) : Query
    {
        $columns = is_array($columns) ? $columns : [$columns];
        $as = is_array($as) ? $as : [$as];
        $string = [];
        foreach ($columns as $key=>$column){
            $string[] = "any_value({$column}) ".($as[$key] ?? '');
        }
        return $this->addSelect(join(",", $string));
    }

    /**
     * 关联查询
     * @param $tableName
     * @param string|array $on
     * @param string $type
     * @return Query
     */
    public function join($tableName, $on, $type = 'inner') : Query
    {
        $status = "default";
        $as = "";
        if($tableName instanceof Query){
            //->join(DB::from(''))...
            list($queryString, $params) = $tableName->compileToQueryString();
            $this->grammar->flushParams($params);
            $as = $tableName->joinAs;
            if(empty($as)){
                DB::log("关联查询时时，缺少别名声明", $this->connection->debug);
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
                DB::log("关联查询时时，缺少别名声明", $this->connection->debug);
                DB::log("\r\n对应部分SQL语句：{$queryString}", $this->connection->debug);
            }
            $table = "({$queryString}) {$as}";
            $status = "nested";
        }else{
            $table = $tableName;
        }
        $this->joins[] = compact("table", "as", "on", "type", "status");
        return $this;
    }

    /**
     * inner join 关联查询
     * @param $tableName
     * @param $on
     * @return Query
     */
    public function innerJoin($tableName, $on) : Query
    {
        return $this->join($tableName, $on);
    }

    /**
     * left join 关联查询
     * @param $tableName
     * @param $on
     * @return Query
     */
    public function leftJoin($tableName, $on) : Query
    {
        return $this->join($tableName, $on, 'left');
    }

    /**
     * right join 关联查询
     * @param $tableName
     * @param $on
     * @return Query
     */
    public function rightJoin($tableName, $on) : Query
    {
        return $this->join($tableName, $on, 'right');
    }

    /**
     * 关联查询时，需要声明临时表的别名时使用
     * @param string $as
     * @return Query
     */
    public function joinAs(string $as) : Query
    {
        $this->joinAs = $as;
        return $this;
    }

    /**
     * 增删查改的where条件
     * @param $column
     * @param null $operator
     * @param null $value
     * @param string $boolean
     * @return Query
     */
    public function where($column, $operator=null, $value=null, $boolean='and') : Query
    {
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
            DB::log("所输入的对比符号不正确", $this->connection->debug);
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
     * @return Query
     */
    public function orWhere($column, $operator=null, $value=null) : Query
    {
        if(func_num_args()==2){
            list($value, $operator) = array($operator, "=");
        }
        return $this->where($column, $operator, $value, 'or');
    }

    /**
     * 原生条件语句
     * @param $sql
     * @param string $boolean
     * @return $this
     */
    public function whereRaw($sql, $boolean='and') : Query
    {
        $type = 'Raw';
        $this->wheres[] = compact('type', 'sql', 'boolean');
        return $this;
    }

    /**
     * or + 原生条件语句
     * @param $sql
     * @return Query
     */
    public function orWhereRaw($sql) : Query
    {
        return $this->whereRaw($sql, 'or');
    }

    /**
     * 添加一个子查询
     * @param $column
     * @param $operator
     * @param Closure $callback
     * @param $boolean
     * @return Query
     */
    public function whereSub($column, $operator, Closure $callback, $boolean='and') : Query
    {
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
     * @return Query
     */
    public function whereNested(Closure $callBack, $boolean='and') : Query
    {
        $query = $this->newQuery();
        $query->from($this->tableName);
        call_user_func($callBack, $query);
        return $this->addNestedWhereQuery($query, $boolean);
    }

    /**
     *
     * @param $query
     * @param string $boolean
     * @return Query
     */
    public function addNestedWhereQuery($query, $boolean='and') : Query
    {
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
     * @return Query
     */
    public function whereNull($column, $boolean='and', $not=false) : Query
    {
        $type = $not ? 'NotNull' : 'Null';
        $this->wheres[] = compact('type', 'column', 'boolean');
        return $this;
    }

    /**
     * or 字段为 null
     * @param $column
     * @param bool $not
     * @return Query
     */
    public function orWhereNull($column, $not=false) : Query
    {
        return $this->whereNull($column, 'or', $not);
    }

    /**
     * 字段不为null
     * @param $column
     * @param string $boolean
     * @return Query
     */
    public function whereNotNull($column, $boolean='and') : Query
    {
        return $this->whereNull($column, $boolean, true);
    }

    /**
     * or 字段不为null
     * @param $column
     * @return Query
     */
    public function orWhereNotNull($column) : Query
    {
        return $this->orWhereNull($column, true);
    }

    /**
     * 字段值是否为空的条件
     * @param $column
     * @param string $boolean
     * @param bool $not
     * @return Query
     */
    public function whereEmpty($column, $boolean='and', $not=false) : Query
    {
        return $this->where(function($eq) use($column, $not, $boolean){
            $eq->where($column, $not ? '!=' : '=', '', $boolean);
            if($not){
                $eq->whereNull($column, 'and', true);
            }else{
                $eq->orWhereNull($column, false);
            }
        }, null, null, $boolean);
    }

    /**
     * or 字段为空
     * @param $column
     * @param bool $not
     * @return Query
     */
    public function orWhereEmpty($column, $not=false) : Query
    {
        return $this->whereEmpty($column, 'or', $not);
    }

    /**
     * 字段不为空
     * @param $column
     * @param string $boolean
     * @return Query
     */
    public function whereNotEmpty($column, $boolean='and') : Query
    {
        return $this->whereEmpty($column, $boolean, true);
    }

    /**
     * or 字段不为空
     * @param $column
     * @return Query
     */
    public function orWhereNotEmpty($column) : Query
    {
        return $this->orWhereEmpty($column, true);
    }

    /**
     * @param $column
     * @param $value
     * @param string $boolean
     * @param bool $not
     * @return Query
     */
    public function whereIn($column, $value, $boolean='and', $not=false) : Query
    {
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
     * @return Query
     */
    public function whereInSub($column, Closure $callback, $boolean='and', $not=false) : Query
    {
        $type = $not ? 'NotIn' : 'In';
        $query = $this->newQuery();
        call_user_func($callback, $query);
        $this->wheres[] = compact('type', 'column', 'query', 'boolean');
        return $this;
    }

    /**
     * not in
     * @param $column
     * @param $value
     * @param string $boolean
     * @return Query
     */
    public function whereNotIn($column, $value, $boolean='and') : Query
    {
        return $this->whereIn($column, $value, $boolean, true);
    }

    /**
     * or in
     * @param $column
     * @param $value
     * @param bool $not
     * @return Query
     */
    public function orWhereIn($column, $value, $not=false) : Query
    {
        return $this->whereIn($column, $value, 'or', $not);
    }

    /**
     * or not in
     * @param $column
     * @param $value
     * @return Query
     */
    public function orWhereNotIn($column, $value) : Query
    {
        return $this->orWhereIn($column, $value, true);
    }

    /**
     * between 条件
     * @param $column
     * @param $value
     * @param string $boolean
     * @param bool $not
     * @return Query
     */
    public function whereBetween($column, $value, $boolean='and', $not=false) : Query
    {
        $type = $not ? 'NotBetween' : 'Between';
        $this->wheres[] = compact('type', 'column', 'value', 'boolean');
        return $this;
    }

    /**
     * xx not between a and b
     * @param $column
     * @param $value
     * @param string $boolean
     * @return Query
     */
    public function whereNotBetween($column, $value, $boolean='and') : Query
    {
        return $this->whereBetween($column, $value, $boolean, true);
    }

    /**
     * or xx between a and b
     * @param $column
     * @param $value
     * @param bool $not
     * @return Query
     */
    public function orWhereBetween($column, $value, $not=false) : Query
    {
        return $this->whereBetween($column, $value, 'or', $not);
    }

    /**
     * or xx not between a and b
     * @param $column
     * @param $value
     * @return Query
     */
    public function orWhereNotBetween($column, $value) : Query
    {
        return $this->orWhereBetween($column, $value, true);
    }

    /**
     * exists条件
     * @param $value
     * @param string $boolean
     * @param bool $not
     * @return Query
     */
    public function whereExists($value, $boolean='and', $not=false) : Query
    {
        if($value instanceof Closure){
            return $this->whereExistsSub($value, $boolean, $not);
        }else{
            return $this;
        }
    }

    /**
     * not exists
     * @param $value
     * @param string $boolean
     * @return Query
     */
    public function whereNotExists($value, $boolean='and') : Query
    {
        return $this->whereExists($value, $boolean, true);
    }

    /**
     * or exists
     * @param $value
     * @param bool $not
     * @return Query
     */
    public function orWhereExists($value, $not=false) : Query
    {
        return $this->whereExists($value, 'or', $not);
    }

    /**
     * or not exists
     * @param $value
     * @return Query
     */
    public function orWhereNotExists($value) : Query
    {
        return $this->orWhereExists($value, true);
    }

    /**
     * exists (sql ...)
     * @param Closure $callback
     * @param string $boolean
     * @param bool $not
     * @return Query
     */
    public function whereExistsSub(Closure $callback, $boolean='and', $not=false) : Query
    {
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
     * @return Query
     */
    public function whereFindInSet($value, $column, $boolean='and') : Query
    {
        $type = 'FindInSet';
        $this->wheres[] = compact('type', 'column', 'value', 'boolean');
        return $this;
    }

    /**
     * or find_in_set
     * @param $value
     * @param $column
     * @return Query
     */
    public function orWhereFindInSet($value, $column) : Query
    {
        return $this->whereFindInSet($value, $column, 'or');
    }

    /**
     * like 查询条件
     * @param $column
     * @param $value
     * @param string $boolean
     * @param bool $not
     * @return Query
     */
    public function whereLike($column, $value, $boolean='and', $not=false) : Query
    {
        $type = $not ? 'NotLike' : 'Like';
        $this->wheres[] = compact('type', 'column', 'value', 'boolean');
        return $this;
    }

    /**
     * or like
     * @param $column
     * @param $value
     * @param bool $not
     * @return Query
     */
    public function orWhereLike($column, $value, $not=false) : Query
    {
        return $this->whereLike($column, $value, "or", $not);
    }

    /**
     * not like
     * @param $column
     * @param $value
     * @return Query
     */
    public function whereNotLike($column, $value) : Query
    {
        return $this->whereLike($column, $value, 'and', true);
    }

    /**
     * or not like
     * @param $column
     * @param $value
     * @return Query
     */
    public function orWhereNotLike($column, $value) : Query
    {
        return $this->orWhereLike($column, $value, true);
    }

    /**
     * 正则匹配
     * @param $column
     * @param $value
     * @param string $boolean
     * @param bool $not
     * @return Query
     */
    public function whereRegexp($column, $value, $boolean='and', $not=false) : Query
    {
        $type = $not ? 'NotRegexp' : 'Regexp';
        $this->wheres[] = compact('type', 'column', 'value', 'boolean');
        return $this;
    }

    /**
     * or regexp
     * @param $column
     * @param $value
     * @param bool $not
     * @return Query
     */
    public function orWhereRegexp($column, $value, $not=false) : Query
    {
        return $this->whereRegexp($column, $value, "or", $not);
    }

    /**
     * not regexp
     * @param $column
     * @param $value
     * @return Query
     */
    public function whereNotRegexp($column, $value) : Query
    {
        return $this->whereRegexp($column, $value, 'and', true);
    }

    /**
     * or not regexp
     * @param $column
     * @param $value
     * @return Query
     */
    public function orWhereNotRegexp($column, $value) : Query
    {
        return $this->orWhereRegexp($column, $value, true);
    }

    /**
     * 判断以 逗号 , 分隔的字符与字段值是否 有交集
     * @param string $column
     * @param string|array $value
     * @param bool $not
     * @return Query
     */
    public function whereIsIntersection(string $column, $value, $not = false) : Query
    {
        $value = is_array($value) ? join(",", $value) : $value;
        if(!empty($value) && !empty($column)){
            return $this->whereRaw("(select concat('{$value}', ',') regexp concat(replace({$column}, ',', ',|'), ',')) = ".($not ? "0" : "1"));
        }
        return $this;
    }

    /**
     * 判断以 逗号 , 分隔的字符与字段值是否 无交集
     * @param string $column
     * @param string|array $value
     * @return Query
     */
    public function whereNotIntersection(string $column, $value) : Query
    {
        return $this->whereIsIntersection($column, $value, true);
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
     * @return Query
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
     * @return Query
     */
    public function whereDateTimeStartAt($column, $start, $datetime=true){
        $start = $datetime ? $this->translateToDatetime($start) : $start;
        return $this->where($column, ">=", $start);
    }

    /**
     * 到什么时间结束
     * @param $column
     * @param $end
     * @param boolean $datetime 字段是否是datetime类型，如果不是，默认使用int 10位长度去处理
     * @return Query
     */
    public function whereDateTimeEndAt($column, $end, $datetime=true){
        $end = $datetime ? $this->translateToDatetime($end) : $end;
        return $this->where($column, "<=", $end);
    }

    /**
     * 查询，过去N个月
     * @param $column
     * @param $number
     * @param boolean $datetime 字段是否是datetime类型，如果不是，默认使用int 10位长度去处理
     * @return Query
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
     * @return Query
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
     * @return Query
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
     * @return Query
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
     * @return Query
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
     * @return Query
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
     * @return Query
     */
    public function whereDay($column, $number=0, $datetime=true){
        $column = $datetime ? $column : "from_unixtime({$column})";
        return $this->whereRaw("TO_DAYS(NOW())-TO_DAYS({$column})<={$number}");
    }

    /**
     * union
     * @param Query $query
     * @param bool $all
     * @return Query
     */
    public function union(Query $query, $all=false) : Query
    {
        $this->unions[] = compact('query', 'all');
        return $this;
    }

    /**
     * union all
     * @param Query $query
     * @return Query
     */
    public function unionAll(Query $query){
        return $this->union($query);
    }

    /**
     * 侧重查询
     * @param $column
     * @param $operator
     * @param null $value
     * @return Query
     */
    public function having($column, $operator, $value=null) : Query
    {
        if(func_num_args()==2){
            list($value, $operator) = [$operator, '='];
        }
        $this->having[] = compact('column', 'operator', 'value');
        return $this;
    }

    /**
     * group by
     * @param $columns
     * @return Query
     */
    public function groupBy($columns) : Query
    {
        $this->groupBy[] = $columns;
        return $this;
    }

    /**
     * order by
     * @param $column
     * @param null $type
     * @return Query
     */
    public function orderBy($column, $type=null) : Query
    {
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
     * @return array|bool
     */
    public function get($total=null, $offset=0){
        $result = $this->grammar->get($total, $offset);
        if(false === $result){
            list($queryString, $params) = $this->compileToQueryString();
            DB::log("SQL 语法错误：\r\n{$queryString}\r\n参数：{".implode(",",$params)."}\r\n", $this->connection->debug);
        }
        return $result;
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

    /**
     * 读取前X条
     * @param $total
     * @return Query|bool
     */
    public function top($total){
        return $this->get($total);
    }

    /**
     * 获取行数
     * @return int
     */
    public function count(){
        return $this->grammar->count();
    }

    /**
     * 获取行数
     * @return int
     */
    public function rows(){
        return $this->count();
    }

    /**
     * @param $column
     * @param null $value
     * @param bool $valueIncludeField
     * @return Query
     */
    public function set($column, $value=null, $valueIncludeField=false) : Query
    {
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
     * @return bool | Query
     */
    public function update($columns=null)
    {
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
     * @return bool | Query
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
     * @return bool | Query
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
     * @return Query|bool
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
     * @return bool | Query
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
     * @return bool|Query
     */
    public function copyTo($table, array $unCopyFields=array(), array $resetFields=array(), $total=1, $offset=0, $fn=null){
        $data = $this->get($total, $offset);
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
        if(!is_null($this->connection)){
            return $this->connection->getError();
        }
        return null;
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
        $this->isAggregateQuery = true;
        $row = $this->first();
        return isset($row["val"]) ? $row["val"] : "";
    }

    /**
     * 检测是否属于聚合查询
     * @return bool
     */
    public function checkAggregateQuery(){
        if($this->isAggregateQuery){
            return true;
        }
        $columns = $this->columns;
        $columnsString = join(",", $columns);
        return strripos($columnsString, "sum(")
            || strripos($columnsString, "avg(")
            || strripos($columnsString, "count(")
            || strripos($columnsString, "max(")
            || strripos($columnsString, "min(");
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
        return $this->aggregateFunction($column, "avg");
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
     * @return bool
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
     * 原生SQL赋值
     * @param $column
     * @param $value
     * @param null $set
     * @return Query
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
     * @return Query
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
     * @return Query
     */
    public function decrement($column, $number=1, $set=null){
        if($number<0){return $this->increment($column, abs($number), $set);}
        return $this->setRaw($column, "`{$column}`-{$number}", $set);
    }

    /**
     * 获取数据库连接
     * @return Connection
     */
    public function getConnection(){
        if(is_null($this->connection)){
            $this->connection = DB::getConnection();
        }
        return $this->connection;
    }

    /**
     * 获取PDO
     * @param string $type
     * @return false|PDO
     */
    public function getPdo($type = 'read'){
        return $this->getConnection()->getPdo($type);
    }
}