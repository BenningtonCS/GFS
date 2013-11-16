<?php

$hostsFile = file_get_contents('/data/GFS/hosts.txt');
$activeHostsFile = file_get_contents('/data/GFS/activehosts.txt');
$logFile = file_get_contents('/data/GFS/masterLog.log');


$hostsList = explode("\n", $hostsFile);
$activeHostsList = explode("\n", $activeHostsFile);

$hostString = '';

foreach($hostsList as $key => $value) {
	if(in_array($value, $activeHostsList)) {
		$hostString .= '<tr><td>' .$value. '</td><td><span class="label label-success">ONLINE</span></td>';
	} 
	else {
		$hostString .= '<tr><td>' .$value. '</td><td><span class="label label-warning">OFFLINE</span></td>';
	}
	$hostString .= '<td>TBA</td><td><a href="chunkserver.php?server='.$value.'">Details</td></tr>';
}
?>
<h2>Master Statistics</h2>
<p><strong>IP Address: </strong><?=$_SERVER['SERVER_ADDR']?></p>
<h2>Chunkservers</h2>
<div class="table-responsive">
	<table class="table table-hover">
		<tr>
			<th>Server IP</th>
			<th>Status</th>
			<th>Space Usage</th>
			<th>Details</th>
		</tr>
		<?=$hostString?>
	</table>
</div>
<h2>Master Log</h2>
<textarea class="form-control" rows="15"><?=$logFile?></textarea>
