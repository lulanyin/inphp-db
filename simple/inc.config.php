<?php
/**
 * Created by PhpStorm.
 * Date: 2017/6/17
 * Time: PM5:12
 * 服务器必须开启的功能有以下这些：
 * PHP扩展：PDO_MYSQL, mbstring
 * PHP版本要求：5.5+，建议 7.0+
 * MYSQL版本要求：5.5+
 */
error_reporting(E_ALL);
/**
 * 启用自动加载机制
 * 自动加载机制，请注意大小写，按默认名称大小写加载，或者自动转换为全部小写去加载路径，linux区分大小写，请注意！
 */
spl_autoload_register(function($fileName){
    $fileName = str_replace('\\', '/', $fileName);
    $fileName = strripos($fileName, '/')===0 ? $fileName : ("/".$fileName);
    $dir = str_replace('\\', '/', __DIR__);
    $url = $dir.$fileName;
    //按当前文件夹
    if(is_file($url.".class.php")){
        require_once $url.".class.php";
    }elseif(is_file(strtolower($url).".class.php")){
        require_once strtolower($url).".class.php";
    }elseif(is_file($url.'.php')){
        require_once $url.'.php';
    }elseif(is_file(strtolower($url).'.php')){
        require_once strtolower($url).'.php';
    }else{
        $url = $dir."/../..".$fileName;
        if(is_file($url.".class.php")){
            require_once $url.".class.php";
        }elseif(is_file(strtolower($url).".class.php")){
            require_once strtolower($url).".class.php";
        }elseif(is_file($url.'.php')){
            require_once $url.'.php';
        }elseif(is_file(strtolower($url).'.php')){
            require_once strtolower($url).'.php';
        }
    }
});