<?php

class GFS {
	public $x;
	public function __construct() {
		$this->x = stream_socket_client("tcp://10.10.117.109:6666", $errno, $errorMessage);
	}
	public function create($filename) {
		fwrite($this->x, "CREATE|".$filename);
	}
	public function append($filename, $data) {
		fwrite($this->x, "APPEND|".$filename."|".$data);
	}

}
$gfs = new GFS;
$gfs->create('hello/2');
$gfs->append('hello/2', '123')
?>
