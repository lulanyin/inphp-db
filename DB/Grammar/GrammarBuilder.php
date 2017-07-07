<?php
/**
 * Created by PhpStorm.
 * Date: 2017/6/17
 * Time: PM5:59
 */
namespace DB\Grammar{

    use DB\Query\QueryBuilder;
    use PDO;
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
         * 获取查询结果
         * @param int $total
         * @param int $offset
         * @return array|bool
         */
        public function get($total = 1000, $offset = 0)
        {
            $queryString = $this->compileToQueryString();
            $queryString .= " limit {$offset},{$total}";
            $self = $this;
            $read_pdo = $self->query->connection->getPdo('read');
            try{
                $result = $read_pdo->query($queryString);
                $result->setFetchMode($this->query->fetchModel);
                return $result->fetchAll();
            }catch(PDOException $e){
                $self->query->error = "code : ".$e->getCode()."<br>error : ".$e->getMessage()."<br>query : ".$queryString;
                $self->query->errorId = $e->getCode();
                return false;
            }
        }

        /**
         * 查询数据量
         * @param string $queryString
         * @return int
         */
        public function count($queryString=''){
            $columns = $this->query->columns;
            $this->query->columns = ["count('')"];
            $queryString = !empty($queryString) ? $queryString : $this->compileToQueryString();
            $this->query->columns = $columns;
            if($statement = $this->read_pdo->query($queryString)){
                //return $statement->rowCount();
                $result = $statement->fetchAll();
                return !empty($result) ? end($result)[0] : 0;
            }else{
                return 0;
            }
        }

        /**
         * 执行 INSERT INTO 语句
         * @return bool|GrammarBuilder
         */
        public function insert()
        {
            // TODO: Implement insert() method.
            $query = $this->compileToQueryString('insert');
            //print_r($query);exit();
            $this->query->connection->beginTransaction();
            foreach($query as $q){
                list($queryString, $params) = $q;
                if(!$this->execute($queryString, 'insert', $params)){
                    $this->query->connection->rollBack();
                    return false;
                }
            }
            $this->query->connection->commit();
            return $this;
        }

        /**
         * 执行 UPDATE 语句
         * @return bool|GrammarBuilder
         */
        public function update()
        {
            // TODO: Implement update() method.
            list($queryString, $params) = $this->compileToQueryString('update');
            return $this->execute($queryString, 'update', $params);
        }

        /**
         * 执行 DELETE 语句
         * @return bool|GrammarBuilder
         */
        public function delete()
        {
            // TODO: Implement delete() method.
            $queryString = $this->compileToQueryString('delete');
            return $this->execute($queryString, 'delete');
        }

        /**
         * 执行SQL语句
         * @param $queryString
         * @param string $type
         * @param array $params
         * @return GrammarBuilder|bool
         */
        public function execute(&$queryString, $type, array $params = [])
        {
            // TODO: Implement execute() method.
            $write_pdo = $this->query->connection->getPdo('write');
            try{
                switch($type){
                    case "delete" :
                        $affectRows = $write_pdo->exec($queryString);
                        if($affectRows===false){
                            throw new PDOException($write_pdo->errorInfo()[2]."<br>query : {$queryString}<br>code source : ".$write_pdo->errorCode(), intval($write_pdo->errorCode()));//注意，errorCode()有可能不是一个数字，所以要强制转换
                        }else{
                            $this->query->affectRows += $affectRows;
                        }
                        break;
                    case "update" :
                    case "insert" :
                    case "truncate" :
                        $statement = $write_pdo->prepare($queryString);
                        if($statement->execute($params)){
                            if($type=='insert'){
                                $this->query->lastInsertId[] = $write_pdo->lastInsertId();
                            }
                            $this->query->affectRows += $statement->rowCount();
                        }else{
                            $code = $statement->errorCode();
                            throw new PDOException($statement->errorInfo()[2]."<br>query : ".$statement->queryString."<br>code source : ".$code, intval($code));
                        }
                        break;
                    default :
                        throw new PDOException("error type", 38);
                        break;
                }
            }catch (PDOException $e){
                $this->query->setError($e->getMessage(), $e->getCode());
                Log::log("Query : {$queryString}\r\nError : ".$e->getMessage()."\r\n", 'sql');
                return false;
            }
            return $this;
        }

        /**
         * 重置表
         * @return bool|GrammarBuilder
         */
        public function truncate(){
            $tableName = $this->compileTable('write');
            $queryString = "truncate table {$tableName}";
            return $this->execute($queryString, 'truncate');
        }

        /**
         * 解析完整的sql语句
         * @param string $type
         * @return string | array
         */
        public function compileToQueryString($type = 'select')
        {
            // TODO: Implement compileToQueryString() method.
            $tableName = $this->compileTable($type=='select' ? 'read' : 'write');
            switch($type){
                case "delete" :
                    $where = $this->compileWhere($type);
                    $where = !empty($where) ? " where {$where}" : "";
                    return "delete from {$tableName}{$where}";
                    break;
                case "update" :
                    $where = $this->compileWhere($type);
                    $where = !empty($where) ? " where {$where}" : "";
                    list($columns, $values) = $this->compileSet($type);
                    return ["update {$tableName} set {$columns}{$where}", $values];
                    break;
                case "insert" :
                    $inserts = $this->compileSet($type);
                    $query = [];
                    foreach($inserts as $ins){
                        list($columns, $values, $params) = $ins;
                        $query[] = ["insert into {$tableName} ({$columns}) values ({$values})", $params];
                    }
                    return $query;
                    break;
                default :
                    $columns = $this->compileColumns();
                    $join = $this->compileJoin();
                    $where = $this->compileWhere($type);
                    $where = !empty($where) ? " where {$where}" : "";
                    $groupBy = $this->compileGroupBy();
                    $orderBy = $this->compileOrderBy();
                    $having = $this->compileHaving();
                    $union = $this->compileUnion();
                    return "select {$columns} from {$tableName}{$join}{$where}{$groupBy}{$orderBy}{$having}{$union}";
                    break;
            }
        }

        /**
         * 解析要查询的字段
         * @return string
         */
        public function compileColumns(){
            $columns = $this->query->columns;
            $columnString = join(",", $columns);
            if((strripos($columnString, '*')>0 || strripos($columnString, '*')===0) && !strripos($columnString, '.*')){
                return "*";
            }else{
                return $columnString;
            }
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
         * 解析 JOIN
         * @return string
         */
        public function compileJoin(){
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
         * 将条件解析成字符串
         * @param string $type
         * @return string
         */
        public function compileWhere($type='select'){
            $wheres = $this->query->wheres;
            $string = [];
            foreach($wheres as $w){
                switch($w['type']){
                    case "Sub" :
                        if(isset($w['query']) && $w['query'] instanceof QueryBuilder){
                            $sqlStr = $w['query']->compileToQueryString();
                            $string[] = [
                                'string' => !empty($sqlStr) ? "({$sqlStr})" : "",
                                'boolean' => $w['boolean']
                            ];
                        }
                        break;
                    case "Nested" :
                        $sqlStr = $this->compileWhereNested($w);
                        $string[] = [
                            'string' => !empty($sqlStr) ? "({$sqlStr})" : "",
                            'boolean' => $w['boolean']
                        ];
                        break;
                    case "Raw" :
                        $string[] = [
                            'string' => !empty($w['sql']) ? $w['sql'] : "",
                            'boolean' => $w['boolean']
                        ];
                        break;
                    default :
                        $string[] = $this->compileWhereString($w);
                        break;
                }
            }
            if(!empty($string)){
                $whereString = [];
                foreach($string as $key=>$str){
                    if(!empty($str['string'])){
                        $boolean = strtolower($str['boolean']);
                        $boolean = in_array($boolean, ['or', 'and']) ? $boolean : 'and';
                        $whereString[] = ($key===0 ? "" : (!empty($whereString[$key-1]) ? " " : "").$boolean." ").$str['string'];
                    }
                }
                return !empty($whereString) ? join("", $whereString) : "";
            }
            return "";
        }

        /**
         * 将条件解析成字符串
         * @param array $where
         * @param $type
         * @return array
         */
        private function compileWhereString(array $where, $type='select'){
            switch($where['type']){
                case "NotIn" :
                case "In" :
                    if(isset($where['query']) && ($where['query'] instanceof QueryBuilder)){
                        $value = $where['query']->compileToQueryString();
                    }else{
                        $value = $where['value'];
                        $value = is_array($value) || is_object($value) ? join(",", $value) : $value;
                    }
                    return [
                        "string" => "{$where['column']} ".($where['type']=='NotIn' ? 'not ' : '')."in ({$value})",
                        "boolean" => $where['boolean']
                    ];
                    break;
                case "Null" :
                case "NotNull" :
                    return [
                        "string" => "{$where['column']} is ".($where['type']=='NotNull' ? "not " : "")."null",
                        "boolean" => $where['boolean']
                    ];
                    break;
                case "NotBetween" :
                case "Between" :
                    $value = $where['value'];
                    $value = is_array($value) ? join(" and ", $value) : $value;
                    return [
                        "string" => "{$where['column']} ".($where['type']=='NotBetween' ? "not " : "")."between {$value}",
                        "boolean" => $where['boolean']
                    ];
                    break;
                case "NotExists" :
                case "Exists" :
                    if(isset($where['query']) && ($where['query'] instanceof QueryBuilder)){
                        $value = $where['query']->compileToQueryString();
                        return [
                            "string" => ($where['type']=='NotExists' ? 'not ' : '')."exists ({$value})",
                            "boolean" => $where['boolean']
                        ];
                    }else{
                        return ['string'=>'', 'boolean'=>$where['boolean']];
                    }
                    break;
                case "FindInSet" :
                    return [
                        'string' => "find_in_set({$where['value']}, {$where['column']})",
                        "boolean" => $where['boolean']
                    ];
                    break;
                case "NotLike" :
                case "Like" :
                    return [
                        'string' => "{$where['column']} ".($where['type']=='NotLike' ? 'not' : '')."like '{$where['value']}'",
                        'boolean' => $where['boolean']
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
                        'string' => "{$where['column']}{$where['operator']}$value",
                        'boolean' => $where['boolean']
                    ];
                    break;
            }
        }

        /**
         * 解析嵌套条件
         * @param array $where
         * @param $type
         * @return string
         */
        private function compileWhereNested(array $where, $type='select'){
            if(isset($where['query']) && ($where['query'] instanceof QueryBuilder)){
                return $where['query']->grammar->compileWhere('select');
            }
            return "";
        }

        /**
         * 处理 set
         * @param $type
         * @return array
         */
        public function compileSet(&$type){
            switch($type){
                case "update" :
                    $columns = $values = [];
                    $sets = is_array(end($this->query->set)) ? end($this->query->set) : $this->query->set;
                    foreach($sets as $set){
                        $columns[] = "`{$set['field']}`=".($set['include_field']===true ? $set['value'] : ":{$set['field']}");
                        if($set['include_field']===false){
                            $values[] = $set['value'];
                        }
                    }
                    return [join(",",$columns), $values];
                    break;
                case "insert" :
                    $sets = is_array(end($this->query->set)) ? $this->query->set : [$this->query->set];
                    $return = [];
                    foreach($sets as $key=>$set_array){
                        $columns = $values = $params = [];
                        foreach($set_array as $set){
                            $columns[] = "`{$set['field']}`";
                            $values[] = $set['include_field']===true ? $set['value'] : ":{$set['field']}";
                            if($set['include_field']===false){
                                $params[] = $set["value"];
                            }
                        }
                        $return[] = [join(",",$columns), join(",",$values), $params];
                    }
                    return $return;
                    break;
                default :
                    return array('', [], []);
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
         * @return string
         */
        public function compileUnion(){
            $unions = $this->query->unions;
            $string = [];
            foreach($unions as $u){
                $sql = $u['query']->grammar->compileToQueryString();
                if(!empty($sql)){
                    $string[] = "union ".($u['all'] ? 'all ' : '')."({$sql})";
                }
            }
            return !empty($string) ? " ".join(" ",$string) : "";
        }
    }
}