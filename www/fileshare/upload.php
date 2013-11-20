<?php
include 'gfs.class.php';
$gfs = new GFS;
  if ($_FILES["file"]["error"] > 0)
    {
    echo "Return Code: " . $_FILES["file"]["error"] . "<br>";
    }
  else
    {
    echo "Upload: " . $_FILES["file"]["name"] . "<br>";
    echo "Type: " . $_FILES["file"]["type"] . "<br>";
    echo "Size: " . ($_FILES["file"]["size"] / 1024) . " kB<br>";
    echo "Temp file: " . $_FILES["file"]["tmp_name"] . "<br>";

    if (file_exists("files/" . $_FILES["file"]["name"]))
      {
      echo $_FILES["file"]["name"] . " already exists. ";
      }
    else
      {
      move_uploaded_file($_FILES["file"]["tmp_name"],
      "files/" . $_FILES["file"]["name"]);
      echo "Stored in: " . "files/" . $_FILES["file"]["name"];
      $gfs->create($_FILES["file"]["name"]);
      }
    }
//include 'pages/header.php';
//include 'pages/upload.php';
//include 'pages/footer.php';

?>
