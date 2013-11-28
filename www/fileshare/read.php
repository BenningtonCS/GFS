<?php

include 'gfs.class.php';
//include 'pages/header.php';
$gfs = new GFS;

$gfs->read("10.jpg");
//$read = explode("|", $read);
//if($read[0] == $_GET['fileName']) {
//	$link = $read[1];
//}
//else {
//	$link = "#";
//}
?>
<a href="#">Download File</a>
<?php

//include 'pages/footer.php';

?>