<?php
include 'gfs.class.php';
$gfs = new GFS;
$files = $gfs->fileList();
include 'pages/header.php';
$files = explode("|", $files);
unset($files[0]);
?>
<h2>File Manager</h2>
<table class="table table-hover">
	<tr>
		<th>File Name</th>
		<th>Chunk Handle</th>
		<th>Locations</th>
		<th>Download</th>
	</tr>
<?php
foreach($files as $file) {
	echo '<tr>';
	$item = explode("^", $file);
	$name = $item[0];
	unset($item[0]);
	echo '<td>'.$name.'</td>';
	$i = 0;
	foreach($item as $x) {
		if($i == 0) {
			$item2 = explode("*", $x);
			echo '<td>'.$item2[0].'</td>';
			unset($item2[0]);
			echo '<td>';
			foreach($item2 as $x) {
				echo $x.'<br>';
			}
			echo '</td><td><a href="file.php?fileName='.$name.'">Download File</a></td>';
		}
		else {
			echo '</tr><tr><td></td>';
			$item2 = explode("*", $x);
			echo '<td>'.$item2[0].'</td>';
			unset($item2[0]);
			echo '<td>';
			foreach($item2 as $x) {
				echo $x.'<br>';
			}
			echo '</td><td></td>';
		}
		$i++;

	}
}
?>
</table>
<?php
include 'pages/footer.php';
?>
