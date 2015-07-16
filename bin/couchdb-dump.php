#!/usr/bin/env php
<?php

ini_set('memory_limit', '-1');
fwrite(STDERR, "COUCH DB DUMPER | version: 1.2.0" . PHP_EOL);
fwrite(STDERR, "(c) Copyright 2013, Anton Bondar <anton@zebooka.com> http://zebooka.com/soft/LICENSE/" . PHP_EOL . PHP_EOL);
fwrite(STDERR, "(c) Copyright 2014, Updated by Miralem Mehic <miralem@mehic.info>. Sponsored by CloudPBX Inc. <info@cloudpbx.ca>" . PHP_EOL . PHP_EOL);

$help = <<<HELP
   This tool dumps available documents it can find using _all_docs request to CouchDB.
   Dump format is compatible with _bulk_docs feature in CouchDB.

OPTIONS:
   -h                 Display this help message.
   -e                 Turn php error reporting ON.
   -H <HOSTNAME>      Hostname or IP of CouchDB server (default: 'localhost').
   -p <PORT>          Port of CouchDB server (default: 5984).
   -d <DATABASE>      Database to dump.
   -g                 Download all databases from the server.
   -z                 Compress output group directory in .tar.gz archive
   -a                 Fetch attachments inline (capture them in base64 encoded format).
   -X                 No revisions history in dump.
   -A                 Fetch attachments binary (Download them to current folder).
   -s                 Outputs each document to separate file inside database directory in current folder (title of directory is the same as the title of database)
   -t                 Used with -s to add timestamp mark to the folder
   -m                 Allowing multiprocessing (works only on UNIX/LINUX platform)
   -P                 Pretty JSON output
   -y <PHP_FILE>      Include this PHP script that returns callback/function to check if document/revision needs to be dumped.

USAGE:
   {$_SERVER['argv'][0]} -H localhost -p 5984 -d test > dump.json
HELP;



class Dumper{

    private $host;
    private $port;
    private $database;
    private $noHistory;
    private $callbackFile;
    private $inlineAttachment;
    private $binaryAttachments;
    private $prettyJsonOutput;
    private $separateFiles;
    private $callbackFilter; 
    private $fp;
    private $backupFolder;

    function Dumper( 
        $host, 
        $port, 
        $database,
        $noHistory, 
        $callbackFile, 
        $inlineAttachment, 
        $binaryAttachments, 
        $prettyJsonOutput, 
        $separateFiles, 
        $timestamp, 
        $callbackFilter,
        $backupFolder = ""
    ){
        
        if (!isset($database) || '' === $database) {
            fwrite(STDERR, "ERROR: Please specify database name (-d <DATABASE>)." . PHP_EOL);
            exit(1);
        }

        $this->host = $host;
        $this->port = $port;
        $this->database = urlencode($database); 
        $this->noHistory = $noHistory;
        $this->callbackFile = $callbackFile;
        $this->inlineAttachment = $inlineAttachment;
        $this->binaryAttachments = $binaryAttachments;
        $this->prettyJsonOutput = $prettyJsonOutput;
        $this->separateFiles = $separateFiles;
        $this->callbackFilter = $callbackFilter;
        $this->backupFolder = $backupFolder;

        $this->databaseName = $backupFolder . "/" . urlencode(($timestamp) ? $database . '-' . date('Y-m-d_H-i-s') . '_UTC'  : $database);
        $this->databaseName = $this->databaseName;

        $fileName = $backupFolder . "/" . $this->database . '.json';
 
        if(!$this->separateFiles)
            $this->fp = fopen($fileName,"w");
    }   

    public function download(){

        // get all docs IDs
        $url = "http://{$this->host}:{$this->port}/" .  $this->database . "/_all_docs"; 
        fwrite(STDERR, "Fetching all documents info from db '{$this->database}' at {$this->host}:{$this->port} ..." . PHP_EOL);
        $curl = getCommonCurl($url);
        $result = trim(curl_exec($curl));
        $statusCode = curl_getinfo($curl, CURLINFO_HTTP_CODE);
        curl_close($curl);
        if (200 == $statusCode) {
            $all_docs = json_decode($result, true);
        } else {
            // unknown status
            fwrite(STDERR, "ERROR: Unsupported response when fetching all documents info from db '{$this->database}' (http status code = {$this->statusCode}) " . PHP_EOL);
            return; //exit(2);
        }

        if (!isset($all_docs['rows']) || !count($all_docs['rows']) || !is_array($all_docs['rows'])) {
            fwrite(STDERR, "ERROR: No documents found in db '{$this->database}'." . PHP_EOL);
            return; //exit(2);
        }

        if(!$this->separateFiles){
            // first part of dump
            if (!$this->noHistory) {
                fwrite($this->fp, '{"new_edits":false,"docs":[' . PHP_EOL);
            } else {
                fwrite($this->fp, '{"docs":[' . PHP_EOL);
            }   
        }


        $first = true;
        $count = count($all_docs['rows']);
        fwrite(STDERR, "Found {$count} documents..." . PHP_EOL);
        
        $i = 1;
        foreach ($all_docs['rows'] as $doc) {
            
            // foreach DOC get all revs
            if (!$this->noHistory) {
                $url = "http://{$this->host}:{$this->port}/{$this->database}/" . urlencode($doc['id']) . "?revs=true&revs_info=true" . (($this->inlineAttachment) ? "&attachments=true" : "");
            } else {  
                $url = "http://{$this->host}:{$this->port}/{$this->database}/" . urlencode($doc['id']) . (($this->inlineAttachment || $this->binaryAttachments) ? "?attachments=true" : "");
            }

            //fwrite(STDERR, "[{$doc['id']}]");
            $percentage =  round(  ($i++/sizeof($all_docs['rows']))*100   , 2) ; 
            fwrite(STDERR, "Processing database \"$this->database\": $percentage%");
         
            $curl = getCommonCurl($url);
             
            curl_setopt($curl, CURLOPT_HTTPHEADER, array(
                'Content-type: application/json',
                'Accept: *\/*'
            ));
              
            $result = $wholeDocument = curl_exec($curl); 
            $statusCode = curl_getinfo($curl, CURLINFO_HTTP_CODE);
            curl_close($curl);

            if (200 == $statusCode) {
              
               $doc_revs = json_decode($result);
               $doc_revs = (array)$doc_revs;

            } else {
                // unknown status
                fwrite(STDERR, "ERROR: Unsupported response when fetching document [{$doc['id']}] from db '{$this->database}' (http status code = {$statusCode}) " . PHP_EOL);
                return; //exit(2);
            }

            //REVISIONS
            if (isset($doc_revs['_revs_info']) && count($doc_revs['_revs_info']) > 1) {

                $revs_info = toArray($doc_revs["_revs_info"]);
                $revs_info = clearEmptyKey($revs_info);

                fwrite(STDERR, "" . PHP_EOL);
                // we have more than one revision
                $revs_info = array_reverse( $revs_info );
                $lastRev = end($revs_info);
                $lastRev = $lastRev['rev'];
                reset($revs_info);

                foreach ($revs_info as $rev) {

                    // foreach rev fetch DB/ID?rev=REV&revs=true
                    //fwrite(STDERR, "[{$doc['id']}] @ {$rev['rev']}");
                    if ('available' === $rev['status']) {
                        $url = "http://{$this->host}:{$this->port}/{$this->database}/" . urlencode($doc['id']) . "?revs=true&rev=" . urlencode($rev['rev']);
                        $curl = getCommonCurl($url);
                        $result = curl_exec($curl);
                        $statusCode = curl_getinfo($curl, CURLINFO_HTTP_CODE);
                        curl_close($curl);
                        if (200 == $statusCode) {
                            $full_doc = trim($result);
                        } else {
                            // unknown status
                            fwrite(STDERR, "ERROR: Unsupported response when fetching document [{$doc['id']}] revision [{$rev['rev']}] from db '{$this->database}' (http status code = {$statusCode}) " . PHP_EOL);
                            return; //exit(2);
                        }
                        if (is_callable($this->callbackFilter) && !call_user_func($this->callbackFilter, json_decode($full_doc, true), $lastRev)) {
                            fwrite(STDERR, " = skipped" . PHP_EOL);
                            continue; // skip that doc version because callback returned false
                        } else {
                            //fwrite(STDERR, "" . PHP_EOL);
                        }
                    } elseif ('missing' === $rev['status']) {
                        //fwrite(STDERR, " = missing" . PHP_EOL);
                        continue; // missing docs are not available anyhow
                    } elseif ('deleted' === $rev['status']) {
                        //fwrite(STDERR, " = deleted" . PHP_EOL);
                        continue; // we will never get deleted docs as we do not have them in _all_docs list
                    } else {
                        //fwrite(STDERR, " = unsupported revision status" . PHP_EOL);
                        continue; // who knows :)
                    }
                    
                    if($this->prettyJsonOutput)
                        $full_doc = indent($full_doc);
            
                    //if we want to save each document in separate file
                    if($this->separateFiles){ 
 
                        if (!file_exists('./' .  $this->databaseName)) 
                            mkdir('./' . $this->databaseName , 0777, true);
         
                        $myfile = fopen("./" . $this->databaseName . "/" . $doc['id'] . '_rev' . $rev['rev'] . ".json", "w");
                        fwrite($myfile, $full_doc);
                        fclose($myfile);

                     //Or if we want to join them together
                    }else{
                        // add document to dump
                        if (!$first) {
                            fwrite($this->fp, ', ' . PHP_EOL . $full_doc);
                        } else {
                            fwrite($this->fp, $full_doc);
                        }
                        $first = false;
                    }
                }
          
            //NO REVISIONS
            } else {
               
                // we have only one revision
                unset($doc_revs['_revs_info']);
                $lastRev = $doc_revs['_rev'];
                if (is_callable($this->callbackFilter) && !call_user_func($this->callbackFilter, $doc_revs, $lastRev)) {
                    fwrite(STDERR, " = skipped" . PHP_EOL);
                    continue; // skip that doc version because callback returned false
                } else {
                    fwrite(STDERR, "" . PHP_EOL);
                }
                if ($this->noHistory) {
                    unset($doc_revs['_rev']);
                }

                if((!$this->inlineAttachment && !$this->binaryAttachments))
                    unset($doc_revs["_attachments"]);

                $doc_revs = clearEmptyKey($doc_revs);
                $full_doc = json_encode($doc_revs, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES); 
 
                $doc_revs = toArray($doc_revs);

                if($this->binaryAttachments && !$this->inlineAttachment && isset($doc_revs["_attachments"]) && $doc_revs["_attachments"]){ 
                    foreach($doc_revs["_attachments"] as $key=>$value){
                        $doc_revs["_attachments"][$key]["length"] = strlen($value["data"]);
                        $doc_revs["_attachments"][$key]["stub"] = true;
                        unset($doc_revs["_attachments"][$key]["data"]);
                    }  
                }
              
                if($this->prettyJsonOutput)
                    $full_doc = indent($full_doc);
 
                //IF we want to save each document in separate file
                if($this->separateFiles){
                    
                    if (!file_exists('./' . $this->databaseName)) 
                        mkdir('./' . $this->databaseName, 0777, true);
 
                    $myfile = fopen("./" . $this->databaseName . "/" . $doc['id']. ".json", "wb");

                    if($myfile != false){
                        fwrite($myfile, $full_doc);
                        fclose($myfile);
                    }

                //Or if we want to join them together..
                }else{
         
                    if ($full_doc !== null && $full_doc !== false) {
                        if (!$first) {
                            fwrite($this->fp, ', ' . PHP_EOL . $full_doc);
                        } else {
                            fwrite($this->fp, $full_doc);
                        }
                        $first = false;
                    }   
                } 

                /* 
                *   Binary attachments 
                */
                if($this->binaryAttachments && $doc_revs["_attachments"]){

                    foreach($doc_revs["_attachments"] as $attachment_id => $content){ 

                        $tempUrl = "http://{$this->host}:{$this->port}/{$this->database}/" . urlencode($doc['id']) . "/" . urlencode($attachment_id);  
                        $folder = $this->databaseName . '/' . $doc['id'];  
                        
                        if (!file_exists('./' . $folder)) 
                            mkdir('./' . $folder, 0777, true);
                    
                        $ch = getCommonCurl( $tempUrl );
                        $fp = fopen( './' . $folder . '/' . $attachment_id, 'wb'); //download attachment to current folder
                        curl_setopt($ch, CURLOPT_FILE, $fp);
                        curl_setopt($ch, CURLOPT_HEADER, 0);
                        curl_exec($ch);
                        curl_close($ch);
                        fclose($fp); 
                    }
                } 
            }
          
        }

        // end of dump
        if(!$this->separateFiles)
            fwrite($this->fp, PHP_EOL . ']}' . PHP_EOL);

        if($this->fp)
        fclose($this->fp);

        return; //exit(0); 
    } 
}/* END OF CLASS */



$params = parseParameters($_SERVER['argv'], array('H', 'p', 'd', 'y','m' ));
error_reporting(!empty($params['e']) ? -1 : 0);
defined('JSON_UNESCAPED_SLASHES') || define('JSON_UNESCAPED_SLASHES', '0');
defined('JSON_UNESCAPED_UNICODE') || define('JSON_UNESCAPED_UNICODE', '0');

if (isset($params['h'])) {
    fwrite(STDERR, $help . PHP_EOL);
    exit(1);
}

$groupDownload = isset($params['g']) ? strval($params['g']) : false;
$host = isset($params['H']) ? trim($params['H']) : 'localhost';
$port = isset($params['p']) ? intval($params['p']) : 5984;
$database = isset($params['d']) ? strval($params['d']) : null;
$noHistory = isset($params['X']) ? $params['X'] : false;
$callbackFile = isset($params['y']) ? $params['y'] : null;
$inlineAttachment = isset($params['a']) ? $params['a'] : false; 
$binaryAttachments = (isset($params['A']) && $noHistory) ? $params['A'] : false;
$prettyJsonOutput = (isset($params['P'])) ? $params['P'] : false;
$separateFiles = (isset($params['s'])) ? $params['s'] : false;
$timeStamp = (isset($params['t'])) ? $params['t'] : false;
$multiprocessing  = (isset($params['m'])) ? intval($params['m']) : 0;
$compressData  = (isset($params['z'])) ? $params['z'] : false;
$callbackFilter = null;
  
if (null !== $callbackFile) {
    $callbackFilter = include $callbackFile;
    if (!is_callable($callbackFilter)) {
        fwrite(STDERR, "ERROR: PHP script with filter callback/function must return valid callable." . PHP_EOL);
        exit(1);
    }
}

if ('' === $host || $port < 1 || 65535 < $port) {
    fwrite(STDERR, "ERROR: Please specify valid hostname and port (-H <HOSTNAME> and -p <PORT>)." . PHP_EOL);
    exit(1);
}

if (isset($params['A']) && !$noHistory) {
    fwrite(STDERR, "ERROR: In order to fetch attachments binary, you must use -X option." . PHP_EOL);
    exit(1);
}


if($groupDownload){

    //Separate files is included for all databases automatically
    $separateFiles = 1;

    fwrite(STDERR, "GROUP DOWNLOAD STARTED" . PHP_EOL);

    // get all docs IDs
    $url = "http://{$host}:{$port}/_all_dbs";
    fwrite(STDERR, "Fetching all databases from {$host}:{$port} ..." . PHP_EOL);
  

    $curl = getCommonCurl($url);
    $result = trim(curl_exec($curl));
    $statusCode = curl_getinfo($curl, CURLINFO_HTTP_CODE);
    curl_close($curl);


    if (200 == $statusCode) {
        $all_docs = json_decode($result, true);
    } else {
        // unknown status
        fwrite(STDERR, "ERROR: Unsupported response when fetching all documents info from db '{$database}' (http status code = {$statusCode}) " . PHP_EOL);
        return; //exit(2);
    }

    try{
  
        $i = 1;
        $backupFolder = "backup_" . strtolower(gmdate("l")) . gmdate("_j-m-Y_h_i_s_e");
        foreach($all_docs as $db){ 
            $allowMultiprocessing = false;

            if(substr($db, 0, 1) != '_'){

                if($multiprocessing || $i < $multiprocessing){
                    
                    $allowMultiprocessing = true; 
                    $i++;

                    $pid = pcntl_fork(); 
                }else 
                    $pid = 0;

                if (!$pid) {

                    $dumper = new Dumper( 
                        $host, 
                        $port, 
                        $db,  
                        $noHistory, 
                        $callbackFile, 
                        $inlineAttachment, 
                        $binaryAttachments, 
                        $prettyJsonOutput, 
                        $separateFiles, 
                        $timeStamp, 
                        $callbackFilter,
                        $backupFolder
                    );  
                    $dumper->download();   

                    if($allowMultiprocessing)
                        exit;   
                } 
            }
        }
 
        if($multiprocessing){
            
            foreach($all_docs as $db){
                 pcntl_wait($status);
            }

            /*
            while (pcntl_waitpid(0, $status) != -1) {
                $status = pcntl_wexitstatus($status);
                echo "Child $status completed\n";
            }
            */
        }

        if($compressData){
          //Compres file
          $a = new PharData( $backupFolder . '.tar'); 
          $a->buildFromDirectory(dirname(__FILE__) . '/' . $backupFolder);    
          file_put_contents( $backupFolder . '.tar.gz' , gzencode(file_get_contents( $backupFolder . '.tar')));

          //remove other files
          deleteDir( realpath($backupFolder) );
          unlink( realpath($backupFolder . '.tar') ); 
        }    

    }catch(Exception $e){
        fwrite(STDERR, "$e" . PHP_EOL);
    }

    return; //exit(1);

}else{
 

    $dumper = new Dumper( 
        $host, 
        $port, 
        $database,  
        $noHistory, 
        $callbackFile, 
        $inlineAttachment, 
        $binaryAttachments, 
        $prettyJsonOutput, 
        $separateFiles, 
        $timeStamp, 
        $callbackFilter
    ); 
    $dumper->download();
 
}



function deleteDir($dirPath) {
    if (! is_dir($dirPath)) {
        throw new InvalidArgumentException("$dirPath must be a directory");
    }
    if (substr($dirPath, strlen($dirPath) - 1, 1) != '/') {
        $dirPath .= '/';
    }
    $files = glob($dirPath . '*', GLOB_MARK);
    foreach ($files as $file) {
        if (is_dir($file)) {
            deleteDir($file);
        } else {
            unlink($file);
        }
    }
    rmdir($dirPath);
}


////////////////////////////////////////////////////////////////////////////////

function getCommonCurl($url)
{
    $curl = curl_init();
    curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($curl, CURLOPT_TIMEOUT, 60);
    curl_setopt($curl, CURLOPT_USERAGENT, 'Mozilla/5.0 curl');
    curl_setopt($curl, CURLOPT_FOLLOWLOCATION, true);
    curl_setopt($curl, CURLOPT_MAXREDIRS, 3);
    curl_setopt($curl, CURLOPT_BINARYTRANSFER, 1);
    curl_setopt($curl, CURLOPT_URL, $url);
    return $curl;
}

////////////////////////////////////////////////////////////////////////////////

/**
 * Convert incoming object to array (deep inspection, recursive function)
 * @author Miralem Mehic <miralem@mehic.info>
 * @param array $obj Incoming object 
 * @return array
 */
function toArray($obj)
{
    if (is_object($obj)) $obj = (array)$obj;
    if (is_array($obj)) {
        $new = array();
        foreach ($obj as $key => $val) {
            $new[$key] = toArray($val);
        }
    } else {
        $new = $obj;
    }

    return $new;
}

////////////////////////////////////////////////////////////////////////////////

/**
 * Parse incoming parameters like from $_SERVER['argv'] array.
 * @author Anton Bondar <anton@zebooka.com>
 * @param array $params Incoming parameters
 * @param array $reqs Parameters with required value
 * @param array $multiple Parameters that may come multiple times
 * @return array
 */
function parseParameters(array $params, array $reqs = array(), array $multiple = array())
{
    $result = array();
    reset($params);
    while (list(, $p) = each($params)) {
        if ($p[0] == '-' && $p != '-' && $p != '--') {
            $pname = substr($p, 1);
            $value = true;
            if ($pname[0] == '-') {
                // long-opt (--<param>)
                $pname = substr($pname, 1);
                if (strpos($p, '=') !== false) {
                    // value specified inline (--<param>=<value>)
                    list($pname, $value) = explode('=', substr($p, 2), 2);
                }
            }
            $nextparam = current($params);
            if ($value === true && in_array($pname, $reqs)) {
                if ($nextparam !== false) {
                    list(, $value) = each($params);
                } else {
                    $value = false;
                } // required value for option not found
            }
            if (in_array($pname, $multiple) && isset($result[$pname])) {
                if (!is_array($result[$pname])) {
                    $result[$pname] = array($result[$pname]);
                }
                $result[$pname][] = $value;
            } else {
                $result[$pname] = $value;
            }
        } else {
            if ($p == '--') {
                // all next params are not parsed
                while (list(, $p) = each($params)) {
                    $result[] = $p;
                }
            } else {
                // param doesn't belong to any option
                $result[] = $p;
            }
        }
    }
    return $result;
}

/**
 * Indents a flat JSON string to make it more human-readable.
 *
 * @param string $json The original JSON string to process.
 *
 * @return string Indented version of the original JSON string.
 */
function indent(&$json) {

    $result      = '';
    $pos         = 0;
    $strLen      = strlen($json);
    $indentStr   = '  ';
    $newLine     = "\n";
    $prevChar    = '';
    $outOfQuotes = true;

    for ($i=0; $i<=$strLen; $i++) {

        // Grab the next character in the string.
        $char = substr($json, $i, 1);

        // Are we inside a quoted string?
        if ($char == '"' && $prevChar != '\\') {
            $outOfQuotes = !$outOfQuotes;

        // If this character is the end of an element,
        // output a new line and indent the next line.
        } else if(($char == '}' || $char == ']') && $outOfQuotes) {
            $result .= $newLine;
            $pos --;
            for ($j=0; $j<$pos; $j++) {
                $result .= $indentStr;
            }
        }

        // Add the character to the result string.
        $result .= $char;

        // If the last character was the beginning of an element,
        // output a new line and indent the next line.
        if (($char == ',' || $char == '{' || $char == '[') && $outOfQuotes) {
            $result .= $newLine;
            if ($char == '{' || $char == '[') {
                $pos ++;
            }

            for ($j = 0; $j < $pos; $j++) {
                $result .= $indentStr;
            }
        }
        $prevChar = $char;
    }

    return $result;
}

function clearEmptyKey($input){

    if(!is_array($input))
        $input = toArray($input); 
  
    foreach($input as $key=>$val){
 
        if(is_array($val))
         $val = clearEmptyKey($val); 

        if($key == "_empty_"){
            $input[""] = $val;      
            unset($input[$key]); 
        }
    }
    return $input; 
}