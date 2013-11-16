<?php

function createChart($name, $values) {
	$string = '';
	$string .= <<<EOD <script type="text/javascript">
  window.onload = function () {
    var '.$name.'Chart = new CanvasJS.Chart("'.$name.'",
    {

      title:{
      text: "CPU | By Minute"
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

    .'$name'.Chart.render(); 
	EOT;
	$string .= '<div class="'.$name.'"></div>';
	return $string;
}

$test = createChart('test', 'test');
echo $test;
