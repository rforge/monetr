<?php

/**
* Connect to a database and perform a query.
*/

require 'php_monetdb.php';

$db =  monetdb_connect($lang = "sql", $host = "127.0.0.1", $port = 50000, $username = "monetdb", $password = "monetdb", $database = "mydb",$hashfunc="sha1");

$fp = fopen('php.tsv', 'w');

for ($n = 1000; $n <= 100000; $n+=1000) {

  print($n."\n");
  
  $start = microtime(true);
  
  $res = monetdb_query($db, monetdb_escape_string("select * from partsupp limit $n")) or trigger_error(monetdb_last_error());

  $result = array();
  while ( $row = monetdb_fetch_object($res) )
  {
	  #var_dump($row);
	  $result[] = $row;
  }
  
  print(sizeof($result)."\n");
  
  $time = (microtime(true) - $start);
  print($time."\n");
  
  $rs = $n/$time;
  
  print($rs."\n");
  
  monetdb_free_result($res);
  
  $res = array("n"=>$n,"rs"=>$rs,"time"=>$time);
  
  fputcsv($fp,$res,"\t");
}
fclose($fp);

?>