<?php
include 'gfs.class.php';
$gfs = new GFS;
$rand = rand();
$read = $gfs->read($_GET['fileName'], $rand);
if($read == "1") {
	$link = "download/".$rand.$_GET['fileName'];
}
else {
	$link = "#";
}
echo $link;
?>