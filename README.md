# PHP 数据库操作类

### 安装方式
```
composer require lulanyin/db
```

### 使用方式
```php
//config.php

//定义数据库配置文件或配置，填写文件路径（*推荐*）
define("DB_CONFIG", ROOT."/config/private/mysql.php");

//或者可以直接设置为值
define("DB_CONFIG", ["值请参考下方 return 之后的数组"]);

//数据库配置文件 ROOT."/config/private/mysql.php"
return [
    //是否开启调试
    "debug"         => false,
    //数据遍历方式，具体查看PDO文档
    "fetch_model"   => PDO::FETCH_ASSOC,
    //字符集类型
    "charset"   => "utf8mb4",
    //表名前缀，为了方便使用，读写分离的前缀必须统一
    "prefix"    => 'pre_',
    //超时时间
    'timeout'   => 5,
    //默认连接，若使用读写分离，可以不定义
    "default"       => [
        //服务器地址
        'host'      => '127.0.0.1',
        //端口
        'port'      => 3306,
        //数据库用户名
        'user'      => 'root',
        //密码
        'password'  => '123456',
        //数据库名
        'database'  => 'database'
    ],
    //读分离，若定义了，select 会优先使用
    "read"          => ["数组值请参考上方的 default 连接"],
    //写分离，若定义了，insert update delete 会优先使用
    "write"         => ["数组值请参考上方的 default 连接"],
    //如果有需要 redis, 请定义 redis 连接
    "redis"         => [
        //redis服务器地址
        'host'       => '127.0.0.1',
        //端口
        'port'       => 6379,
        //密码
        'password'   => '',
        //使用第几个库
        'select'     => 0,
        //超时时间
        'timeout'    => 0,
        //过期时间
        'expire'     => 0,
        //持久化
        'persistent' => false,
        //保存的所有 key 的前缀
        'prefix'     => 'redis_pre_',
    ]
];
```

### 支持 Swoole，在 Swoole 服务中请务必设置此常量，数值为连接池的最连接数量
```php
define("DB_SWOOLE_POOLS", 10);
```

### 错误日志文件
```php
//您需要定义一个常量：RUNTIME，并给予此文件夹写入权限，默认保存在 RUNTIME."/db/error.txt" 里边
define("RUNTIME", ROOT."/runtime");
```

### Cache 文件夹的类文件，使用的是 ThinkPHP 中的原文件，当前仅使用到 Redis，其它按需要请自行使用。