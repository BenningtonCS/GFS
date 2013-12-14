<?php
ini_set('meemory_limit', '96M');
ini_set('post_max_size', '64M');
ini_set('upload_max_filesize', '64M');

include 'pages/header.php';
include 'gfs.class.php';

$gfs = new GFS;
  if ($_FILES["file"]["error"] > 0)
    {
    echo "Return Code: " . $_FILES["file"]["error"] . "<br>";
    }
  else
    {
    if (file_exists("files/" . $_FILES["file"]["name"]))
      {
      echo $_FILES["file"]["name"] . " already exists. ";
      }
    else
      {
      move_uploaded_file($_FILES["file"]["tmp_name"],
      "files/" . $_FILES["file"]["name"]);
      $gfs = new GFS;
      $create = $gfs->create($_FILES["file"]["name"]);
      if($create == "CREATE|1") {
        //$filecontents = file_get_contents("files/" . $_FILES["file"]["name"]);
        $append = $gfs->append($_FILES["file"]["name"], "/var/www/fileshare/files/" . $_FILES["file"]["name"]); 
        if($append == "1") {
          unlink("files/" . $_FILES["file"]["name"]);
?>
<h2>File Uploaded Successfully!</h2>
<p>Name: <?=$_FILES["file"]["name"]?></p>
<p>Size: <?=($_FILES["file"]["size"] / 1024)?>Kb</p>
<p>Download Link: <a href="file.php?fileName=<?=$_FILES["file"]["name"]?>">Link</a></p>
<?php 
        }
        else {
          echo 'failed and append is '.$append;
        }
      }
      }
    }
//include 'pages/header.php';
//include 'pages/upload.php';
//include 'pages/footer.php';

?>
