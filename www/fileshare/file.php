<?php

include 'gfs.class.php';
include 'pages/header.php';
$gfs = new GFS;
$filename = $_GET['fileName'];
$ext = substr(strrchr($filename,'.'),1);
if($ext == "mp3") {
	$html = '<div id="player"><audio width=720 height=480></audio></div>';
}
elseif($ext == "mp4") {
	$html = '<div id="player"><video width=720 height=480 src="#" controls="controls"></video></div>';
}
?>
<script type="text/javascript">
    function download() {
    	$("#result").html("Please Wait...")
    	$.get( "download.php", { fileName: '<?=$filename?>' } )
    	.done(function( data ) {
    		window.location = 'http://backus.bennington.edu/fileshare/'+data;
  });
    }
    function stream() {
    	$("#result").html("Please Wait...");
    	$("#result").html('<?=$html?>');

    	$.get( "stream.php", { fileName: '<?=$filename?>' } )
    	.done(function( data ) {
    	$('video,audio').attr('src', 'download/'+data+'<?=$filename?>');
    	$('video,audio').mediaelementplayer(/* Options */);

  });

    }

 
</script>
<h1>File Details | <?=$filename?></h1>
<h2>Options</h2>
<button class="btn btn-primary" onClick="javascript:download()">Download File</button>
<?php
if($ext == "mp3" || $ext == "mp4") {
?>
<button class="btn btn-primary" onClick="stream()">Stream File</button>
<?php
}
?>
<div id="result"></div>
<?php

include 'pages/footer.php';

?>