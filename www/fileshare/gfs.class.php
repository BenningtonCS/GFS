<?php
class GFS {
	public function __construct() {
		public $x = stream_socket_client("tcp://10.10.117.109:6666", $errno, $errorMessage);
	}
	public function create($filename) {
		fwrite($this->x, "CREATE|".$filename);
	}
}
$gfs = new GFS;
$gfs->create('hello');
?>
