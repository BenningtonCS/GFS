<?php

class GFS {
	public $x;
	public $eot = chr(4);
	public function connect() {
		$this->x = stream_socket_client("tcp://10.10.100.144:6666", $errno, $errorMessage);
	}
	public function create($filename) {
		$this->connect();
		fwrite($this->x, "CREATE|".$filename.$eot);
		$response = stream_get_contents($this->x);
		return $response;
	}
	public function append($filename, $data) {
		$this->connect();
		fwrite($this->x, "APPEND|".$filename."|".$data.$eot);
	}
	public function read($filename) {
		$this->connect();
		fwrite($this->x, "READ|".$filename);
		$response = stream_get_contents($this->x);
		return $response;
	}

}
$gfs = new GFS;

?>
