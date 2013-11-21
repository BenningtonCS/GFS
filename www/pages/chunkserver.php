<?php

$server = $_GET['server'];
$logFile = file_get_contents('http://10.10.100.143:8000/listenerLog.log');
$logFile = explode("\n", $logFile);
$cpu = $logFile[0];
$memory = $logFile[1];
$
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
      text: "Network | By Minute"
      },
       data: [
      {
        type: "line",

        dataPoints: [
        { x: 1, y: 450 },
        { x: 2, y: 414 },
        { x: 3, y: 520 },
        { x: 4, y: 460 },
        { x: 5, y: 450 },
        { x: 6, y: 500 } 
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
echo $logFile;
?>
</textarea>
