<?php
namespace DB\Grammar{

    use DB\Query\QueryBuilder;
    use PDO;
    use DB\DB;
    use PDOException;

    class GrammarParamsBuilder{

        /**
         * @var QueryBuilder
         */
        protected $query;
        /**
         * 查询用 PDO
         * @var PDO
         */
        protected $pdo_read;
        /**
         * 增删改用 PDO
         * @var PDO
         */
        protected $pdo_write;
        /**
         * SQL预处理参数化的参数值
         * @var array
         */
        protected $params = [];
        /**
         * 用于预处理
         * @var \PDOStatement | null
         */
        public $statement = null;
        /**
         * PDO是查询还是增删改
         * @var string
         */
        public $pdoModel = 'read';

        public $total = null;
        public $offset = 0;

        public function __construct(QueryBuilder $query){
            $this->query        = $query;
            $this->pdo_read     = $query->connection->getPdo('read');
            $this->pdo_write    = $query->connection->getPdo('write');
        }

        /**
         * 获取 PDO
         * @param string $type
         * @return PDO
         */
        public function getPdo($type='read'){
            return $type=='write' ? $this->pdo_write : $this->pdo_read;
        }

        public function get($total=null, $offset=0){
            $this->total = $total;
            $this->offset = $offset;
            $queryString = $this->compileToQueryString();
            if(!is_null($total)){
                $queryString .= " limit {$offset},{$total}";
            }
            $self = $this;
            $read_pdo = $self->query->connection->getPdo('read');
            try{
                $statement = $read_pdo->prepare($queryString);
                $statement->execute();
                $statement->setFetchMode($this->query->fetchModel);
                return $statement->fetchAll();
            }catch(PDOException $e){
                $self->query->error = "code : ".$e->getCode()."<br>error : ".$e->getMessage()."<br>query : ".$queryString;
                $self->query->errorId = $e->getCode();
                return false;
            }
        }

        public function compileToQueryString($type='select'){
            $this->pdoModel = $type=='select' ? 'read' : 'write';
            $tableName = $this->compileTable($type=='select' ? 'read' : 'write');
            switch ($type){
                case "delete" :
                    //删除, 暂时实现基础的语句： delete from {table name}{where}
                    list($where, $params) = $this->compileWhere();// where里边包含了语句和参数
                    $this->flushParams($params);
                    $queryString = "delete from {$tableName}{$where}";
                    break;
                case "update" :
                    //更新，暂时实现基础的语句：update {table name} set {columns} {where}
                    list($columns, $params1) = $this->compileSet($type);
                    $this->flushParams($params1);
                    list($where, $params2) = $this->compileWhere();// where里边包含了语句和参数
                    $this->flushParams($params2);
                    $queryString = "update {$tableName} set {$columns}{$where}";
                    $params = array_merge($params1, $params2);
                    break;
                case "insert" :
                    //插入，insert into {table name}(columns) values({$values})
                    list($columns, $values, $params) = $this->compileSet($type);
                    $this->flushParams($params);
                    $queryString = "insert into {$tableName} {$columns} values {$values}";
                    break;
                case "insert where not exists" :
                    //插入数据，当记录不存在时
                    //insert into {table name}(columns) select {values} from TEMP1 where not exists(select {table name}{where})
                    list($columns, $values, $params1) = $this->compileSet($type);
                    $this->flushParams($params1);
                    list($where, $params2) = $this->compileWhere();// where里边包含了语句和参数
                    $this->flushParams($params2);
                    $queryString = "insert into {$tableName} {$columns} select {$values} from TEMP1 where not exists(select {$tableName}{$where})";
                    $params = array_merge($params1, $params2);
                    break;
                default :
                    //默认查询
                    $columns    = $this->compileColumns();
                    $join       = $this->compileJoin();
                    list($where, $params1) = $this->compileWhere();
                    $groupBy    = $this->compileGroupBy();
                    $orderBy    = $this->compileOrderBy();
                    $having     = $this->compileHaving();
                    list($union, $params2) = $this->compileUnion();
                    $queryString = "select {$columns} from {$tableName}{$join}{$where}{$groupBy}{$orderBy}{$having}{$union}";
                    $params = array_merge($params1, $params2);
                    break;
            }
            return [$queryString, $params];
        }

        /**
         * 解析查询表名
         * @param string $type
         * @param string $tableName
         * @return string
         */
        public function compileTable($type='read', $tableName=''){
            $read_prefix = $this->query->connection->prefix['read'];
            $write_prefix = $this->query->connection->prefix['write'];
            $table = !empty($tableName) ? $tableName : $this->query->tableName;
            $table = preg_replace("/\s+/"," ",$table);
            $table = trim($table);
            return stripos($table, ".") ? $table : (($type=='write' ? $write_prefix : $read_prefix).$table);
        }

        /**
         * 编译 where 条件
         * @return array
         */
        public function compileWhere(){
            $string = [];
            foreach ($this->query->wheres as $where){
                switch ($where['type']){
                    case "Sub" :
                        if(isset($where['query']) && $where['query'] instanceof QueryBuilder){
                            list($queryString, $params) = $where['query']->compileToQueryString();
                            $string[] = [
                                'string' => !empty($queryString) ? "({$queryString})" : "",
                                'boolean' => $where['boolean'],
                                "params" => $params
                            ];
                        }
                        break;
                    case "Nested" :
                        list($queryString, $params) = $this->compileWhereNested($where);
                        $string[] = [
                            'string' => !empty($queryString) ? "({$queryString})" : "",
                            'boolean' => $where['boolean'],
                            "params" => $params
                        ];
                        break;
                    case "Raw" :
                        $string[] = [
                            'string' => !empty($where['sql']) ? $where['sql'] : "",
                            'boolean' => $where['boolean'],
                            'params' => null
                        ];
                        break;
                    default :
                        $string[] = $this->compileWhereString($where);
                        break;
                }
            }
            if(!empty($string)){
                $whereString = $params = null;
                foreach($string as $key=>$str){
                    if(!empty($str['string'])){
                        //条件 or / and
                        $boolean = strtolower($str['boolean']);
                        $boolean = in_array($boolean, ['or', 'and']) ? $boolean : 'and';
                        //条件语句
                        $whereString[] = ($key===0 ? "" : (!empty($whereString[$key-1]) ? " " : "").$boolean." ").$str['string'];
                        //参数化的值合并
                        if(!empty($str['params'])){
                            $params = array_merge($params, $str['params']);
                        }
                    }
                }
                $where = !empty($whereString) ? ("where ".join("", $whereString)) : null;
                return [$where, $params];
            }
            return [null, null];

        }

        /**
         * 多重查询条件
         * @param $where
         * @return array
         */
        public function compileWhereNested($where){
            if(isset($where['query']) && ($where['query'] instanceof QueryBuilder)){
                return $where['query']->grammar->compileWhere();
            }
            return [null, null];
        }

        /**
         * 将条件解析成字符串
         * @param array $where
         * @return array
         */
        private function compileWhereString(array $where){
            switch($where['type']){
                case "NotIn" :
                case "In" :
                    if(isset($where['query']) && ($where['query'] instanceof QueryBuilder)){
                        list($value, $params) = $where['query']->compileToQueryString();
                    }else{
                        $value = $where['value'];
                        $value = is_array($value) || is_object($value) ? join(",", $value) : $value;
                        $params = [$value];
                        $value = "?";
                    }
                    return [
                        "string" => "{$where['column']} ".($where['type']=='NotIn' ? 'not ' : '')."in ({$value})",
                        "boolean" => $where['boolean'],
                        'params' => $params
                    ];
                    break;
                case "Null" :
                case "NotNull" :
                    return [
                        "string" => "{$where['column']} is ".($where['type']=='NotNull' ? "not " : "")."null",
                        "boolean" => $where['boolean'],
                        'params' => null
                    ];
                    break;
                case "NotBetween" :
                case "Between" :
                    $value = $where['value'];
                    $value = is_array($value) ? join(" and ", $value) : $value;
                    $params = [$value];
                    $value = "?";
                    return [
                        "string" => "{$where['column']} ".($where['type']=='NotBetween' ? "not " : "")."between {$value}",
                        "boolean" => $where['boolean'],
                        'params' => $params
                    ];
                    break;
                case "NotExists" :
                case "Exists" :
                    if(isset($where['query']) && ($where['query'] instanceof QueryBuilder)){
                        list($value, $params) = $where['query']->compileToQueryString();
                        return [
                            "string" => ($where['type']=='NotExists' ? 'not ' : '')."exists ({$value})",
                            "boolean" => $where['boolean'],
                            'params' => $params
                        ];
                    }else{
                        return [
                            'string'=>'',
                            'boolean'=>$where['boolean'],
                            'params' => null
                        ];
                    }
                    break;
                case "FindInSet" :
                    return [
                        'string' => "find_in_set( ?, {$where['column']})",
                        "boolean" => $where['boolean'],
                        'params' => [$where['value']]
                    ];
                    break;
                case "NotLike" :
                case "Like" :
                    return [
                        'string' => "{$where['column']} ".($where['type']=='NotLike' ? 'not' : '')."like '?'",
                        'boolean' => $where['boolean'],
                        'params' => [$where['value']]
                    ];
                    break;
                default :
                    $value = $where['value'];
                    switch($where['operator']){
                        case '>' :
                        case '>=' :
                        case '<' :
                        case '<=' :
                            $value = is_numeric($value) ? $value : "'{$value}'";
                            break;
                        default :
                            $value = "'{$value}'";
                            break;
                    }
                    return [
                        'string' => "{$where['column']}{$where['operator']}?",
                        'boolean' => $where['boolean'],
                        'params' => [$value]
                    ];
                    break;
            }
        }

        /**
         * 编译查询的字段
         * @return string
         */
        private function compileColumns(){
            $columns = $this->query->columns;
            $columnString = join(",", $columns);
            if((strripos($columnString, '*')>0 || strripos($columnString, '*')===0) && !strripos($columnString, '.*')){
                return "*";
            }else{
                return $columnString;
            }
        }

        private function compileJoin(){
            $joins = $this->query->joins;
            $string = [];
            foreach($joins as $j){
                if(!empty($j['table'])){
                    $table = $this->compileTable('read', $j['table']);
                    $string[] = " {$j['type']} join {$table}".($j['status']=='nested' ? $j['as'] : '')." on {$j['column']}{$j['operator']}{$j['value']}";
                }
            }
            return join("", $string);
        }
        /**
         * 把一个值绑定到一个参数
         * @param $parameter
         * @param $value
         * @param int $data_type
         */
        public function bindValue($parameter, $value, $data_type=PDO::PARAM_STR){
            if(is_null($this->statement)){
                DB::log("未初始化 PDOStatement 对象", $this->query->connection->errorDisplay[$this->pdoModel]);
            }
            $this->statement->bindValue($parameter, $value, $data_type);
        }

        /**
         * 刷新参数值
         * @param $addParams
         */
        public function flushParams($addParams){
            $this->params = is_null($this->params) ? [] : $this->params;
            $this->params = !empty($addParams) ? array_merge($this->params, $addParams) : $this->params;
        }

    }
}