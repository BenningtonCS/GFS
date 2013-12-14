<?php

$hostsFile = file_get_contents('/data/PackRat/hosts.txt');
$activeHostsFile = file_get_contents('/data/PackRat/activehosts.txt');
$logFile = file_get_contents('/data/PackRat/masterLog.log');

$hostsList = explode("\n", $hostsFile);
$activeHostsList = explode("\n", $activeHostsFile);

$hostString = '';

foreach($hostsList as $key => $value) {
	if($value != "") {
	if(in_array($value, $activeHostsList)) {
			$hostString .= '<tr><td>' .$value. '</td><td><span class="label label-success">ONLINE</span></td>';
		$spaceFile = file_get_contents('http://'.$value.':8000/listenerLog.log');
		$spaceFile = explode("\n", $spaceFile);
		$spaceLine = $spaceFile[3];
		$spaceString = explode("|", $spaceLine);
		$space = explode('/', $spaceString[59]);
		$used = $space[0];
		$total = $space[1];

	$hostString .= '<td><progress value="'.$used.'" max="'.$total.'"></progress> '.round($used/100000000).' GB/'.round($total/100000000).' GB</td><td><a href="chunkserver.php?server='.$value.'">Details</td></tr>';

		} 
		else{
			$hostString .= '<tr><td>' .$value. '</td><td><span class="label label-warning">OFFLINE</span></td><td><span class="text-warning">Not Available</span></td><td><a href="chunkserver.php?server='.$value.'">Details</td></tr>';
		}
	}
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
