<?php
namespace DB\Grammar{

    use DB\Query\QueryBuilder;
    use PDO;
    use DB\DB;
    use PDOException;

    class GrammarBuilder{

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
        public $params = [];
        public $tempParams = [];
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
        /**
         * 处理结果语句
         * @var array
         */
        public $queryString = [];

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

        /**
         * 查询获取数据
         * @param null $total
         * @param int $offset
         * @return array|bool
         */
        public function get($total=null, $offset=0){
            $this->total = $total;
            $this->offset = $offset;
            list($queryString, $params) = $this->compileToQueryString();
            $this->flushParams($params);
            if(!empty($this->query->result)){
                if($this->query->result_query==$queryString && $params==$this->query->result_query_params){
                    return is_numeric($total) ? array_slice($this->query->result, $offset, min($total, count($this->query->result))) : $this->query->result;
                }
            }
            //判断查询语句、参数是否相同
            if(!is_null($total)){
                $queryString .= " limit {$offset},{$total}";
            }
            $self = $this;
            $read_pdo = $this->getPdo();
            try{
                $this->statement = $read_pdo->prepare($queryString);
                $this->bindValues();
                if($this->statement->execute()){
                    $this->statement->setFetchMode($this->query->fetchModel);
                    return $this->statement->fetchAll();
                }else{
                    $code = intval($this->statement->errorCode());
                    throw new PDOException($this->statement->errorInfo()[2]."<br>query : ".$this->statement->queryString."<br>code source : ".$code, intval($code));
                }
            }catch(PDOException $e){
                $self->query->error = "code : ".$e->getCode()."<br>error : ".$e->getMessage()."<br>query : ".$queryString;
                $self->query->errorId = $e->getCode();
                return false;
            }
        }

        /**
         * 获取记录条数
         * @return int
         */
        public function count(){
            //$columns = $this->query->columns;
            //$this->query->columns = ["count('')"];
            list($queryString, $params) = $this->compileToQueryString();
            $this->flushParams($params);
            //$this->query->columns = $columns;
            $read_pdo = $this->getPdo();
            if($this->statement = $read_pdo->prepare($queryString)){
                $this->bindValues();
                $this->statement->execute();
                $result = $this->statement->fetchAll($this->query->fetchModel);
                //执行行数统计时，要将临时结果存放越来，若后面查询语句不变，则直接使用即可
                $this->query->result = $result;             //查询获得的结果
                $this->query->result_query = $queryString;  //查询语句
                $this->query->result_query_params = $params;//查询语句对应的参数、值
                return !empty($result) && is_array($result) ? count($result) : 0;
            }else{
                return 0;
            }
        }

        /**
         * 执行更改，更改仅一条语句，不成功就是失败，不需要在此定义事务
         * @return bool|GrammarBuilder
         */
        public function update(){
            return $this->execute('update');
        }

        /**
         * 执行插入，插入涉及到多各插入，需要在发生错误时回滚
         * @return bool|GrammarBuilder
         */
        public function insert(){
            list($queryString, $params) = $this->compileToQueryString('insert');
            $this->beginTransaction();
            foreach ($queryString as $key=>$q){
                if(!$this->execute('insert', $q, isset($params[$key]) ? $params[$key] : [])){
                    $this->rollBack();
                    return false;
                }
            }
            $this->commit();
            return $this;
        }

        /**
         * 执行删除，更改仅一条语句，不成功就是失败，不需要在此定义事务
         * @return bool|GrammarBuilder
         */
        public function delete(){
            return $this->execute('delete');
        }

        /**
         * 执行条件式插入，当条件数据不存在时插入，仅可插入一条记录，不成功就是失败，不需要在此定义事务
         * @return bool|GrammarBuilder
         */
        public function insertIfNotExists(){
            return $this->execute('insert where not exists');
        }

        /**
         * 清空表，重新定义自增 ID
         * @return bool|GrammarBuilder
         */
        public function truncate(){
            return $this->execute('truncate');
        }

        /**
         * 执行增删改
         * @param $type
         * @param $queryString
         * @param $params
         * @return bool | GrammarBuilder
         */
        public function execute($type, $queryString=null, $params=null){
            if(is_null($queryString) && is_null($params)){
                list($queryString, $params) = $this->compileToQueryString($type);
            }
            $this->flushParams($params);
            $write_pdo = $this->getPdo('write');
            try{
                $this->statement = $write_pdo->prepare($queryString);
                $this->bindValues();
                if($this->statement->execute()){
                    $this->query->affectRows += $this->statement->rowCount();
                    if(strripos($type, 'insert')===0){
                        $this->query->lastInsertId[] = $write_pdo->lastInsertId();
                    }
                }else{
                    $code = intval($this->statement->errorCode());
                    throw new PDOException($this->statement->errorInfo()[2]."<br>query : ".$this->statement->queryString."<br>code source : ".$code, intval($code));
                }
            }catch (PDOException $e){
                $this->query->connection->setError($e->getMessage(), $e->getCode());
                DB::log("Query : {$queryString}\r\nError : ".$e->getMessage()."\r\n", $this->query->connection->errorDisplay['write']);
                return false;
            }
            return $this;
        }

        /**
         * 开始事务（外部未定义事务时，才开始）
         */
        public function beginTransaction(){
            if(!$this->query->connection->inTransaction){
                $this->pdo_write->beginTransaction();
            }
        }

        /**
         * 事务回滚（外部未定义事务时，才回滚）
         */
        public function rollback(){
            if(!$this->query->connection->inTransaction){
                $this->pdo_write->rollBack();
            }
        }

        /**
         * 提交事务（外部未定义事务时，才提交）
         */
        public function commit(){
            if(!$this->query->connection->inTransaction){
                $this->pdo_write->commit();
            }
        }

        /**
         * 编译查询语句
         * @param string $type
         * @return array
         */
        public function compileToQueryString($type='select'){
            $this->tempParams = $this->params = [];
            $this->pdoModel = $type=='select' ? 'read' : 'write';
            $tableName = $this->compileTable($type=='select' ? 'read' : 'write');
            switch ($type){
                case "delete" :
                    //删除, 暂时实现基础的语句： delete from {table name}{where}
                    list($where, $params) = $this->compileWhere();// where里边包含了语句和参数
                    $this->tempParams = array_merge($this->tempParams, $params);
                    $queryString = "delete from {$tableName}{$where}";
                    break;
                case "update" :
                    //更新，暂时实现基础的语句：update {table name} set {columns} {where}
                    list($columns, $params1) = $this->compileSet($type);
                    $this->tempParams = array_merge($this->tempParams, $params1);
                    list($where, $params2) = $this->compileWhere();// where里边包含了语句和参数
                    $this->tempParams = array_merge($this->tempParams, $params2);
                    $queryString = "update {$tableName} set {$columns}{$where}";
                    $params = array_merge($params1, $params2);
                    break;
                case "insert" :
                    //插入，insert into {table name}(columns) values({$values})
                    list($columns, $values, $params) = $this->compileSet($type);
                    $this->tempParams = array_merge($this->tempParams, $params);
                    $queryString = [];
                    foreach ($columns as $key=>$c){
                        $queryString[] = "insert into {$tableName} {$c} values {$values[$key]};";
                    }
                    break;
                case "insert where not exists" :
                    //插入数据，当记录不存在时
                    //insert into {table name}(columns) select {values} from TEMP1 where not exists(select {table name}{where})
                    list($columns, $values, $params1) = $this->compileSet($type);
                    $this->tempParams = array_merge($this->tempParams, $params1);
                    list($where, $params2) = $this->compileWhere();// where里边包含了语句和参数
                    $this->tempParams = array_merge($this->tempParams, $params2);
                    $queryString = "insert into {$tableName} {$columns} select {$values} from TEMP1 where not exists(select * from {$tableName}{$where});";
                    $params = array_merge($params1, $params2);
                    break;
                case "truncate" :
                    $queryString = "truncate table {$tableName}";
                    $params = [];
                    break;
                default :
                    //默认查询
                    $columns    = $this->compileColumns();
                    $join       = $this->compileJoin();
                    list($where, $params1) = $this->compileWhere();
                    $this->tempParams = array_merge($this->tempParams, $params1);
                    $groupBy    = $this->compileGroupBy();
                    $orderBy    = $this->compileOrderBy();
                    $having     = $this->compileHaving();
                    list($union, $params2) = $this->compileUnion();
                    $this->tempParams = array_merge($this->tempParams, $params2);
                    $queryString = "select {$columns} from {$tableName}{$join}{$where}{$groupBy}{$orderBy}{$having}{$union}";
                    $params = array_merge($params1, $params2);
                    break;
            }
            $this->queryString[$type] = [$queryString, $params];
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
            return stripos($table, ".") || stripos($table, "`")>0 || stripos($table, "`")===0 ? $table : (($type=='write' ? $write_prefix : $read_prefix).$table);
        }

        /**
         * 编译查询的字段
         * @return string
         */
        private function compileColumns(){
            $columns = $this->query->columns;
            $columnString = join(",", $columns);
            return $columnString;
        }

        /**
         * 编译关联查询
         * @return string
         */
        private function compileJoin(){
            $joins = $this->query->joins;
            $string = [];
            foreach($joins as $j){
                if(!empty($j['table'])){
                    $table = $this->compileTable('read', $j['table']);
                    $column = $j['on'][0];
                    $operator = isset($j['on'][2]) ? $j['on'][1] : '=';
                    $value = isset($j['on'][2]) ? $j['on'][2] : $j['on'][1];
                    $string[] = " {$j['type']} join {$table}".($j['status']=='nested' ? $j['as'] : '')." on {$column}{$operator}{$value}";
                }
            }
            return join("", $string);
        }

        /**
         * 编译 where 条件
         * @param bool $Nested
         * @return array
         */
        public function compileWhere($Nested=false){
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
                            'params' => []
                        ];
                        break;
                    default :
                        $string[] = $this->compileWhereString($where);
                        break;
                }
            }
            if(!empty($string)){
                $whereString = $params = [];
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
                $where = !empty($whereString) ? (($Nested ? "" : " where ").join("", $whereString)) : null;
                return [$where, $params];
            }
            return [null, []];

        }

        /**
         * 多重查询条件
         * @param $where
         * @return array
         */
        public function compileWhereNested($where){
            if(isset($where['query']) && ($where['query'] instanceof QueryBuilder)){
                return $where['query']->grammar->compileWhere(true);
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
                        $params = [];
                        $value = $where['value'];
                        $value = is_array($value) || is_object($value) ? $value : explode(",", $value);
                        $string = [];
                        foreach ($value as $key=>$val){
                            $fieldName = $this->getTempParamName($where['column']."_".$key);
                            $params[$fieldName] = $val;
                            $string[] = ":{$fieldName}";
                        }
                        $value = join(",", $string);
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
                        'params' => []
                    ];
                    break;
                case "NotBetween" :
                case "Between" :
                    $params = [];
                    $value = $where['value'];
                    $value = is_array($value) ? $value : explode(" and ", $value);
                    $fieldName1 = $this->getTempParamName($where['column']."_1");
                    $fieldName2 = $this->getTempParamName($where['column']."_2");
                    $params[$fieldName1] = $value[0];
                    $params[$fieldName2] = $value[1];
                    $value = ":{$fieldName1} and :{$fieldName2}";
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
                            'params' => []
                        ];
                    }
                    break;
                case "FindInSet" :
                    $params = [];
                    if(is_numeric($where['value'])){
                        $value = $where['value'];
                    }else{
                        $fieldName = $this->getTempParamName($where['column']);
                        $params[$fieldName] = $where['value'];
                        $value = ":{$fieldName}";
                    }
                    return [
                        'string' => "find_in_set({$value}, {$where['column']})",
                        "boolean" => $where['boolean'],
                        'params' => $params
                    ];
                    break;
                case "NotLike" :
                case "Like" :
                    $params = [];
                    $fieldName = $this->getTempParamName($where['column']);
                    $params[$fieldName] = $where['value'];
                    $value = ":{$fieldName}";
                    return [
                        'string' => "{$where['column']} ".($where['type']=='NotLike' ? 'not' : '')."like {$value}",
                        'boolean' => $where['boolean'],
                        'params' => $params
                    ];
                    break;
                default :
                    $params = [];
                    if(is_numeric($where['value'])){
                        $value = $where['value'];
                    }else {
                        $fieldName = $this->getTempParamName($where['column']);
                        $params[$fieldName] = $where['value'];
                        $value = ":{$fieldName}";
                    }
                    return [
                        'string' => "{$where['column']}{$where['operator']}{$value}",
                        'boolean' => $where['boolean'],
                        'params' => $params
                    ];
                    break;
            }
        }

        /**
         * 解析 group by sql 语句
         * @return string
         */
        public function compileGroupBy(){
            return !empty($this->query->groupBy) ? " group by ".trim(join(",",$this->query->groupBy)) : '';
        }

        /**
         * 解析 order by sql 语句
         * @return string
         */
        public function compileOrderBy(){
            $orderBy = $this->query->orderBy;
            $string = [];
            foreach($orderBy as $o){
                if(!empty($o['column'])){
                    $string[] = preg_replace("/\s+/","",$o['column'])." ".strtolower($o['type']);
                }
            }
            return !empty($string) ? (" order by ".join(",",$string)) : '';
        }

        /**
         * 解析 HAVING 侧重
         * @return string
         */
        public function compileHaving(){
            $having = $this->query->having;
            $string = [];
            foreach($having as $h){
                if(!empty($h['column'])){
                    $string[] = $h['column'].$h['operator'].$h['value'];
                }
            }
            return !empty($string) ? " having ".join(",", $string) : "";
        }

        /**
         * 解析 UNION 联合查询
         * @return array
         */
        public function compileUnion(){
            $unions = $this->query->unions;
            $string = $params = [];
            foreach($unions as $u){
                list($queryString, $params_arr) = $u['query']->grammar->compileToQueryString();
                if(!empty($queryString)){
                    $string[] = "union ".($u['all'] ? 'all ' : '')."({$queryString})";
                    if(!empty($params)){
                        $params = array_merge($params, $params_arr);
                    }
                }
            }
            $queryString = !empty($string) ? " ".join(" ",$string) : "";
            return [$queryString, $params];
        }


        /**
         * 处理 set
         * @param $type
         * @return array
         */
        public function compileSet(&$type){
            switch($type){
                case "update" :
                    //仅支持基础的一条语句更新
                    $columns = $values = $params = [];
                    $sets = is_array(end($this->query->set)) ? end($this->query->set) : $this->query->set;
                    foreach($sets as $set){
                        $fieldName = $this->getTempParamName($set['field']);
                        $columns[] = "`{$set['field']}`=".($set['include_field']===true ? $set['value'] : ":{$fieldName}");
                        if($set['include_field']===false){
                            $params[$fieldName] = $set['value'];
                        }
                    }
                    return [join(",",$columns), $params];
                    break;
                case "insert" :
                    //支持多条记录插入，但每条记录的字段必须一样
                    $sets = is_array(end($this->query->set)) ? $this->query->set : [$this->query->set];
                    $columns = $values = $params = [];
                    foreach($sets as $key=>$set_array){
                        $thisColumns = $thisValues = $thisParams = [];
                        foreach($set_array as $set){
                            $thisColumns[] = "`{$set['field']}`";
                            $thisValues[] = $set['include_field']===true ? $set['value'] : ":{$set['field']}";
                            if($set['include_field']===false){
                                $thisParams["{$set['field']}"] = $set["value"];
                            }
                        }
                        $columns[] = "(".join(",", $thisColumns).")";
                        $values[] = "(".join(",", $thisValues).")";
                        $params[] = $thisParams;
                    }
                    return [$columns, $values, $params];
                    break;
                case "insert where not exists" :
                    //仅支持1条数据插入，请勿多条插入：insert into TABLE (f1, f2, fn) select 'f1_val', 'f2_val', 'fn_val' from dual where not exists (select * from TABLE where {$where})
                    $sets = is_array(end($this->query->set)) ? end($this->query->set) : $this->query->set;
                    $columns = $values = $params = [];
                    foreach($sets as $set){
                        $columns[] = "`{$set['field']}`";
                        $values[] = $set['include_field']===true ? $set['value'] : ":{$set['field']}";
                        if($set['include_field']===false){
                            $params[$set['field']] = $set['value'];
                        }
                    }
                    return ["(".join(",", $columns).")", join(",", $values), $params];
                    break;
                default :
                    return array('', [], []);
                    break;
            }
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
            if(!$this->statement->bindValue($parameter, $value, $data_type)){
                DB::log("参数绑定错误：{$parameter} = {$value}", $this->query->connection->errorDisplay[$this->pdoModel]);
            }
        }

        /**
         * 把所有值绑定到参数
         */
        public function bindValues(){
            if(!empty($this->params)){
                foreach ($this->params as $key=>$val){
                    if(is_numeric($key)){
                        $this->bindValue($key+1, $val);
                    }else{
                        $this->bindValue(":".$key, $val);
                    }
                }
            }
        }

        /**
         * 刷新参数值
         * @param $addParams
         */
        public function flushParams($addParams){
            $this->params = is_null($this->params) ? [] : $this->params;
            $this->params = !empty($addParams) ? array_merge($this->params, $addParams) : $this->params;
        }

        /**
         * 参数名防重复
         * @param $name
         * @return string
         */
        public function getTempParamName($name){
            $name = strtolower($name);
            $name = preg_match("/^[a-z][a-z0-9_]*[a-z0-9]$/i",$name) ? $name : md5($name);
            return isset($this->tempParams[$name]) ? $this->getTempParamName($name."_".count($this->tempParams)) : $name;
        }

    }
}