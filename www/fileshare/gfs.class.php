<?php

class GFS {
	public $x;
	public function __construct() {
		$this->x = stream_socket_client("tcp://10.10.117.109:6666", $errno, $errorMessage);
	}
	public function create($filename) {
		fwrite($this->x, "CREATE|".$filename);
		$response = stream_get_contents($this->x);
		return $response;
	}
	public function append($filename, $data) {
		fwrite($this->x, "APPEND|".$filename."|".$data);
	}

}
$gfs = new GFS;

$create = $gfs->create('hello/5');
if($create == "CREATE|1") {
	$gfs->append('hello/5', '123');	
}

?>
