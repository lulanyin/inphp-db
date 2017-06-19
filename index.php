<?php
/**
 * Created by PhpStorm.
 * Date: 2017/6/17
 * Time: PM5:19
 */
require_once "./inc.config.php";
//引用
use DB\DB;
//开始使用 DB
$connect = new \DB\Connection\Connection([
    'driver' => 'mysql',
    'host' => '127.0.0.1',
    'port' => '3306',
    'user' => 'root',
    'pass' => '123456',
    'database' => 'no498',
    'prefix' => 'n_',
]);
$pdo = $connect->getPdo('write');
$pdo->beginTransaction();
$now = time();
if($pdo->query("insert into n_log(`uid`, `log`, `create_time`, `type`, `ipv4`, `ipv6`, `url`, `post_data`) VALUES (1, 'step 1', '{$now}', 'operating', '127.0.0.1', NULL, NULL, NULL)")){
    if($pdo->query("insert into n_log(`uid`, `log`, `create_time`, `type`, `ipv4`, `ipv6`, `url`, `post_data`) VALUES (1, 'step 2', '{$now}', 'operating', '127.0.0.1', NULL, NULL, NULL)")){
        if($pdo->query("insert into n_log(`uid`, `log`, `create_time`, `type`, `ipv4`, `ipv6`, `url`, `post_data`) VALUES (1, 'step 3', '{$now}', 'operating', '127.0.0.1', NULL, NULL, NULL)")){
            echo "success";
            $pdo->commit();
        }else{
            $pdo->rollBack();
            echo "step 3";
            print_r($pdo->errorInfo());
        }
    }else{
        $pdo->rollBack();
        echo "step 2";
        print_r($pdo->errorInfo());
    }
}else{
    $pdo->rollBack();
    echo "step 1";
    print_r($pdo->errorInfo());
}