<?php
ini_set('meemory_limit', '96M');
ini_set('post_max_size', '64M');
ini_set('upload_max_filesize', '64M');
?>
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">
    <meta name="author" content="">

    <title>GFS Master Server</title>
    <script type="text/javascript" src="js/canvasjs.min.js"></script>
    <script type="text/javascript" src="js/jquery.js"></script>
    <script src="js/mediaelement-and-player.min.js"></script>
    <link rel="stylesheet" href="js/mediaelementplayer.css" />
    <!-- Bootstrap core CSS -->
    <link href="css/bootstrap.css" rel="stylesheet">

    <!-- Custom styles for this template -->
    <link href="css/style.css" rel="stylesheet">
    <!-- HTML5 shim and Respond.js IE8 support of HTML5 elements and media queries -->
    <!--[if lt IE 9]>
      <script src="https://oss.maxcdn.com/libs/html5shiv/3.7.0/html5shiv.js"></script>
      <script src="https://oss.maxcdn.com/libs/respond.js/1.3.0/respond.min.js"></script>
    <![endif]-->
  </head>

  <body>
        <div class="navbar navbar-inverse navbar-fixed-top" role="navigation">
      <div class="container">
        <div class="navbar-header">
          <a class="navbar-brand" href="index.php">Bennington File Share</a>
        </div>
        <div class="navbar-collapse collapse">
          <ul class="nav navbar-nav">
            <li><a href="index.php">Home</a></li>
            <li><a href="files.php">File Manager</a></li>
            <li><a href="http://<?=$_SERVER['SERVER_ADDR']?>">System Stats</a></li>
          </ul>
        </div><!--/.nav-collapse -->
      </div>
    </div>

  <div class="container">
<!--    <div class="page-header">
      <h1>Bennnington File Share</h1>
      <p class="lead">Upload all the files!</p>
    </div>
-->
