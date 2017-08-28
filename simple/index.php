<?php
/**
 * Created by PhpStorm.
 * Date: 2017/6/17
 * Time: PM5:19
 */
require_once "./inc.config.php";
use DB\DB;
//开始使用 DB
$connect =[
    'driver' => 'mysql',
    'host' => '127.0.0.1',
    'port' => '3306',
    'user' => 'root',
    'pass' => 'root',
    'database' => 'zhushou',
    'prefix' => 'm_',
    'error_display' => true
];
DB::init($connect);
DB::beginTransaction();
$db = DB::from('user')->whereIn('uid', [1055, 1056, 1057])->set('sex', '2');
if($db->update()){
    DB::commit();
    echo "数据成功，影响：".$db->getAffectRows()."\r\n";
}else{
    DB::rollBack();
    echo "错误：".$db->getError();
}