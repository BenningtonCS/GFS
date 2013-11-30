<?php

include 'gfs.class.php';
include 'pages/header.php';
$gfs = new GFS;

$read = $gfs->read($_GET['fileName']);
if($read == "1") {
	$link = "download/".$_GET['fileName'];
}
else {
	$link = "#";
}
?>
<h1>Download File</h3>
<a class="btn btn-primary" href="<?=$link?>">Download File</a>
<?php

include 'pages/footer.php';

?>