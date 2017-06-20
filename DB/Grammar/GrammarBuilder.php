<?php
/**
 * Created by PhpStorm.
 * Date: 2017/6/17
 * Time: PM5:59
 */
namespace DB\Grammar{

    use DB\Query\QueryBuilder;
    use PDO;

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


    }
}