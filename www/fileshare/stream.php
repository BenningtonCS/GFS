<?php
include 'gfs.class.php';
$gfs = new GFS;
$rand = rand();
$stream = $gfs->stream($_GET['fileName'], $rand, 0, 63999999, 1);
if($stream == "1") {
	echo $rand;
}
else {
	echo $stream;
}
if(filesize("download/".$rand.$_GET['fileName']) == 64999999) {
$gfs->stream($_GET['fileName'], $rand, 64000000, -1, 0);
}
?>