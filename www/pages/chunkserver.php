<?php

$server = $_GET['server'];
$logFile = file_get_contents('http://'.$server.':8000/httpServerFiles/chunkserverLog.log');
?>
<h2>Chunkserver | <?=$server?></h2>

<h2>Statistics</h2>
<h2>Log</h2>
<textarea class="form-control" rows="15"><?php
echo $logFile;
?>
</textarea>
