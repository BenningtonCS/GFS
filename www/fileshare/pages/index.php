<?php

?>
<h2>Upload</h2>
<form role="form" method="post" enctype="multipart/form-data" action="upload.php">
<div class="form-group">
	<label for="fileName">File Name</label>
	<input type="text" class="form-control" name="fileName" id="fileName" placeholder="Enter a File Name">
</div>
<div class="form-group">
	<label for="file">Select File</label>
	<input type="file" class="form-control" name="file" id="file">
</div>
<button type="submit" class="btn btn-primary">Upload</button>
</form>
