<?php
namespace DB\Grammar;
use DB\Query\QueryBuilder;
use DB\DB;

class mssqlGrammar extends GrammarBuilder {

    private $query;

    public function __construct(QueryBuilder $query)
    {
        $this->query = $query;
        parent::__construct($query);
    }

    /**
     * 获取查询结果
     * @param int $total
     * @param int $offset
     * @return array|bool
     */
    public function get($total = 1000, $offset = 0)
    {
        // TODO: Implement get() method.
        $queryString = $this->compileToQueryString();
        //echo $queryString;exit();
        $read_pdo = parent::getPdo();
        try{
            $result = $read_pdo->query($queryString);
            $result->setFetchMode($this->query->fetchModel);
            $data = $result->fetchAll();
            return DB::fromObject($data)->get($total, $offset);
        }catch(\PDOException $e){
            $this->query->error = "code : ".$e->getCode()."<br>error : ".$e->errorInfo[2]."<br>query : ".$queryString;
            $this->query->errorId = $e->getCode();
            return false;
        }
    }

    public function getArray($total = 1000, $offset = 0){
        return $this->get($total, $offset);
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
        return "[".$this->query->connection->database[$type]."].[dbo].[".(stripos($table, ".") ? $table : (($type=='write' ? $write_prefix : $read_prefix).$table))."]";
    }
}