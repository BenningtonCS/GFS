<?php

$server = $_GET['server'];
$chunkServerLog = file_get_contents('http://'.$server.':8000/httpServerFiles/chunkserverLog.log');
$logFile = file_get_contents('http://'.$server.':8000/listenerLog.log');
$logFile = explode("\n", $logFile);
$cpu = $logFile[0];
$memory = $logFile[1];
$disk = $logFile[3];
?>
<script type="text/javascript">
  window.onload = function () {
    var cpuChart = new CanvasJS.Chart("cpuContainer",
    {

      title:{
      text: "CPU | By Minute"
      },
       data: [
      {
        type: "line",

        dataPoints: [
<?php
$cpu = explode("|", $cpu);
for($i=0;$i<60;$i++) {
  echo '{ x: '.$i.', y: '.$cpu[$i].'}, ';

}
?>
        ]
      }
      ]
    });

    cpuChart.render();

var diskChart = new CanvasJS.Chart("diskContainer",
    {

      title:{
      text: "Memory | By Minute"
      },
       data: [
      {
        type: "line",

        dataPoints: [
<?php
$memory = explode("|", $memory);
for($i=0;$i<60;$i++) {
  echo '{ x: '.$i.', y: '.$memory[$i].'}, ';

}
?>
        ]
      }
      ]
    });

    diskChart.render();


var networkChart = new CanvasJS.Chart("networkContainer",
    {

      title:{
      text: "Disk Usage | By Minute"
      },
       data: [
      {
        type: "line",

        dataPoints: [
<?php
$disk = explode("|", $disk);
for($i=0;$i<60;$i++) {
  $d = explode("/", $disk[$i]);
  echo '{ x: '.$i.', y: '.($d[0]*100/$d[1]).'}, ';

}
?>
        ]
      }
      ]
    });

    networkChart.render();


  }
</script>

<h2>Chunkserver | <?=$server?></h2>

<h2>Statistics</h2>
<div class="row">
	<div class="col-md-4"><div id="cpuContainer" style="height:200px;"></div></div>
        <div class="col-md-4"><div id="diskContainer" style="height:200px;"></div></div>
        <div class="col-md-4"><div id="networkContainer" style="height:200px;"></div></div>
</div>

<h2>Log</h2>
<textarea class="form-control" rows="15"><?php
echo $chunkServerLog;
?>
</textarea>
