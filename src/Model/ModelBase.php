<?php
namespace Inphp\DB\Model;


/**
 * 数据库模型基类
 * Class ModelBase
 * @package Inphp\DB\Model
 */
abstract class ModelBase {

    /**
     * 主键
     * @var string
     */
    protected $primary_key = "id";

    /**
     * Query对象
     * @var \Inphp\DB\Query|\Inphp\DB\Swoole\Query
     */
    protected $db = null;

    /**
     * 主要用于保存数据
     * @var array
     */
    protected $data = [];

    /**
     * 表别名
     * @var null
     */
    protected $as = null;

    /**
     * 表名
     * @var string
     */
    protected $tableName;

    /**
     * 关联的表
     * @var null
     */
    protected $join = null;

    /**
     * 关联条件
     * @var array
     */
    protected $joinWhere = [];

    /**
     * 关联方式
     * @var array
     */
    protected $joinType = [];

    /**
     * 最后的自增ID
     * @var array|int
     */
    public $lastInsertId;

    /**
     * 错误信息
     * @var string
     */
    public $ModelErrorInfo;

    /**
     * 是否属于连接池（主要用于 swoole 扩展）
     * @var bool
     */
    protected $pool = false;

    /**
     * 初始化
     * ModelBase constructor.
     */
    public function __construct(){
        $this->pool = defined("DB_SWOOLE_POOLS");
        $this->db = $this->getNewDB();
    }

    /**
     * 设置数据
     * @param $name
     * @param $value
     */
    public function setAttribute($name, $value){
        $this->data[$name] = $value;
    }

    /**
     * 获取数据
     * @param $name
     * @return mixed|null
     */
    public function getAttribute($name){
        return $this->data[$name] ?? null;
    }

    /**
     * 魔术方法
     * @param $name
     * @param $value
     */
    public function __set($name, $value)
    {
        // TODO: Implement __set() method.
        $this->setAttribute($name, $value);
    }

    /**
     * 获取表名
     * @param boolean $full
     * @return mixed
     */
    public function getTableName($full = false){
        return ($full ? \Inphp\DB\DB::getTablePrefix() : "").$this->tableName;
    }

    /**
     * 动态获取
     * @param $name
     * @return array|mixed
     */
    public function __get($name)
    {
        // TODO: Implement __get() method.
        return $this->getAttribute($name);
    }

    /**
     * 检测数据对象的值
     * @access public
     * @param string $name 名称
     * @return boolean
     */
    public function __isset(string $name)
    {
        try {
            if (array_key_exists($name, $this->data)) {
                return true;
            } else {
                $this->getAttribute($name);
                return true;
            }
        } catch (\InvalidArgumentException $e) {
            return false;
        }

    }

    /**
     * 销毁数据对象的值
     * @access public
     * @param string $name 名称
     * @return void
     */
    public function __unset(string $name)
    {
        unset($this->data[$name]);
    }

    /**
     * 查询别名
     * @param $name
     * @return $this
     */
    public function as_($name){
        $this->as = $name;
        return $this;
    }

    /**
     * 动态重载方法
     * @param $method
     * @param $arguments
     * @return \Inphp\DB\Query|\Inphp\DB\Swoole\Query
     */
    public function __call($method, $arguments)
    {
        // TODO: Implement __call() method.
        $db = $this->mainQuery(1);
        if(!empty($this->join)){
            foreach ($this->join as $k=>$j){
                $db = $db->join($j, $this->joinWhere[$k], $this->joinType[$k]);
            }
        }
        if (method_exists($this, $method)) {
            array_unshift($arguments, $db);
            return call_user_func_array([$this, $method], $arguments);
        } else {
            return call_user_func_array([$db, $method], $arguments);
        }
    }

    /**
     * 开始查询
     * @return \Inphp\DB\Query|\Inphp\DB\Swoole\Query
     */
    public function startQuery(){
        $db = $this->mainQuery(1);
        if(!empty($this->join)){
            foreach ($this->join as $k=>$j){
                $db = $db->join($j, $this->joinWhere[$k], $this->joinType[$k]);
            }
        }
        return $db;
    }

    /**
     * 新数据插入
     * @param array $data
     * @return int
     */
    public function save($data = []){
        if(empty($this->data) && empty($data)){
            $this->ModelErrorInfo = "数据为空，不执行插入";
            return 0;
        }
        $query = $this->db->from($this->tableName);
        $query->set(!empty($this->data) ? array_merge($this->data, $data) : $data);
        if($query->insert()){
            $this->lastInsertId = $query->getLastInsertId();
            return 1;
        }else{
            $this->ModelErrorInfo = $query->getError();
            return 0;
        }
    }

    /**
     * 插入数据
     * @param array $data
     * @return int
     */
    public function submitChange($data = []){
        return $this->save($data);
    }

    /**
     * 获取错误信息
     * @return mixed
     */
    public function getError(){
        return $this->ModelErrorInfo;
    }

    /**
     * 获取一个新的连接
     * @return \Inphp\DB\Query|\Inphp\DB\Swoole\Query
     */
    private function getNewDB(){
        if(!is_null($this->db)){
            return $this->db->newQuery();
        }
        return $this->pool ? (new \Inphp\DB\Swoole\Query()) : (new \Inphp\DB\Query());
    }

    /**
     * 主表的Query对象
     * @param bool $as
     * @return \Inphp\DB\Query|\Inphp\DB\Swoole\Query|null
     */
    public function mainQuery($as=false){
        $db = $this->getNewDB();
        if(!$db){
            return null;
        }
        return $db->newQuery()->from($this->tableName.($as ? (is_string($as) ? " {$as}" : (!empty($this->as) ? " {$this->as}" : "")) : ""));
    }
    public function getQuery($as=0){
        return $this->mainQuery($as);
    }

    /**
     * 返回一个空的Query
     * @return \Inphp\DB\Query|\Inphp\DB\Swoole\Query
     */
    public function emptyQuery(){
        return $this->getNewDB();
    }

    /**
     * 获取一行数据
     * @param null $primary_key
     * @return array
     */
    public static function getRow($primary_key = null){
        $m = new static();
        $db = $m->mainQuery();
        if(!is_null($primary_key)){
            $db->where($m->primary_key, $primary_key);
        }
        return $db->first();
    }

    /**
     * 删除一行数据
     * @param $primary_key
     * @return bool
     */
    public static function delRow($primary_key){
        $m = new static();
        $db = $m->mainQuery()->where($m->primary_key, $primary_key);
        if($db->delete()){
            return $db->getAffectRows();
        }else{
            return false;
        }
    }
}