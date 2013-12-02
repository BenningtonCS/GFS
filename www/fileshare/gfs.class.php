<?php

class GFS {
	public $x;
	public $eot;
	public function connect() {
		$this->x = stream_socket_client("tcp://10.10.100.144:6666", $errno, $errorMessage);
	}
	public function create($filename) {
		$this->connect();
		$message = "CREATE|".$filename;
		$len = strlen($message);
		$buffer = pack("N", $len);
		fwrite($this->x, $buffer.$message);
		$response = stream_get_contents($this->x);
		return $response;
	}
	public function append($filename, $data) {
		$this->connect();
		$message = "APPEND|".$filename."|".$data;
		$len = strlen($message);
		$buffer = pack("N", $len);
		fwrite($this->x, $buffer.$message);
		$response = stream_get_contents($this->x);
		return $response;
		}
	public function read($filename) {
		$this->connect();
		$message = "READ|".$filename;
		$len = strlen($message);
		$buffer = pack("N", $len);
		fwrite($this->x, $buffer.$message);
		$response = stream_get_contents($this->x);
		return $response;
	}

}
?>
