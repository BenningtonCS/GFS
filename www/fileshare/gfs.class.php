<?php

class GFS {
	public $MASTER_IP = '10.10.100.144';
	public function __construct() {
		public $x = stream_socket_client("tcp://$MASTER_IP:6666",)
	}
}

?>
