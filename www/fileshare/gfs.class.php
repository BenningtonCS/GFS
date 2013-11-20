<?php

class GFS {
	public $MASTER_IP = '10.10.117.109';
	public function __construct() {
		public $x = stream_socket_client("tcp://10.10.117.109:6666", $errno, $errorMessage);
		fwrite($x, "hello")
	}
	public function create($filename) {
		fwrite($x, $filename);
	}
}
$gfs = new GFS;
$gfs->create('/rohail/rohail');
?>
