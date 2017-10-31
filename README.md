#PHP数据库操作类
```
<?php
//数据库连接配置 
$config = [
    //默认一个连接池
    'default' => [
        'driver' => 'mysql',        //数据库类型
        'host' => '127.0.0.1',      //数据库服务器地址
        'port' => '3306',           //端口
        'user' => 'root',           //账号
        'pass' => '123456',         //密码
        'database' => 'z_cms',      //数据库名
        'prefix' => 'z_'            //各表的统一前缀
    ]
];
//初始化 DB 类 （本系统中，引用 use DB\DB;将会自动初始化连接数据库）
DB::init($config);
//执行单表查询（所有查询操作，不需要写表的前缀，除非特殊需要！）
//select * from z_user where uid>=15 limit 0,100;
$query = DB::from('user')
    //where方法的第二个参数，默认为 "=" 等于号， 
    ->where('uid', '>=', 15)
    //若 getArray 不填写参数，默认取1000条数据
    ->getArray(100)
//执行多表关联查询
//select u.uid,u.nickname,o.order_id,o.price from user u join order o on u.uid=o.uid where o.state='finish'
$query = DB::from('user u')
    ->join("order o", ['u.uid', 'o.uid'])
    ->where('o.state', 'finish')
    ->select("u.uid,u.nickname,o.order_id,o.price")
    ->getArray();
```