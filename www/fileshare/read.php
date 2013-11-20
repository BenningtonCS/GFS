<?php

include 'gfs.class.php';
include 'pages/header.php';
$gfs = new GFS;

$read = $gfs->read($_GET['fileName']);
$read = explode("|", $read);
if($read[0] == $_GET['fileName']) {
	$link = $read[1];
}
else {
	$link = "#";
}
?>
<a href="<?=$link?>">Download File</a>
<?php

include 'pages/footer.php';

?>