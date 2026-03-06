<?php
// C&CH Panel General - API simple de persistencia
// Guarda/lee un JSON en disco para evitar tener que subir CSVs cada vez.

header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');

$action = isset($_GET['action']) ? $_GET['action'] : (isset($_POST['action']) ? $_POST['action'] : '');

$baseDir = __DIR__ . DIRECTORY_SEPARATOR . 'data';
if (!is_dir($baseDir)) {
  @mkdir($baseDir, 0755, true);
}

$dbFile = $baseDir . DIRECTORY_SEPARATOR . 'database_general.json';
$backupFile = $baseDir . DIRECTORY_SEPARATOR . 'backup_database_general.json';

function blob_path($baseDir, $key){
  // whitelist keys
  $safe = preg_replace('/[^a-z0-9_\-]/i','', $key);
  if($safe === '') return null;
  return $baseDir . DIRECTORY_SEPARATOR . 'blob_' . $safe . '.json';
}

function read_db($file){
  if(!file_exists($file)) return [];
  $raw = @file_get_contents($file);
  if($raw === false) return [];
  $j = json_decode($raw, true);
  return is_array($j) ? $j : [];
}

function write_db($file, $backup, $data){
  // Backup único (rolling) para no llenar el servidor
  if(file_exists($file)){
    @copy($file, $backup);
  }
  $raw = json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
  return @file_put_contents($file, $raw) !== false;
}

function write_blob($file, $backup, $data){
  if(file_exists($file)){
    @copy($file, $backup);
  }
  $raw = json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
  return @file_put_contents($file, $raw) !== false;
}

if($action === 'ping'){
  echo json_encode(['ok' => true]);
  exit;
}


if($action === 'get_full'){
  $db = read_db($dbFile);
  // Blob meta (ofertas/pedidos grandes)
  $blobs = [];
  foreach(['offers','orders'] as $k){
    $p = blob_path($baseDir, $k);
    if($p && file_exists($p)){
      $blobs[$k] = [
        'size' => filesize($p),
        'updatedAt' => gmdate('c', filemtime($p)),
      ];
    } else {
      $blobs[$k] = null;
    }
  }
  echo json_encode(['ok' => true, 'value' => $db, 'blobs' => $blobs]);
  exit;
}

if($action === 'get_blob'){
  $key = isset($_GET['key']) ? $_GET['key'] : '';
  if($key === ''){
    echo json_encode(['ok' => false, 'error' => 'Missing key']);
    exit;
  }
  $p = blob_path($baseDir, $key);
  if(!$p){
    echo json_encode(['ok' => false, 'error' => 'Bad key']);
    exit;
  }
  if(!file_exists($p)){
    echo json_encode(['ok' => true, 'value' => null]);
    exit;
  }
  $raw = @file_get_contents($p);
  $val = json_decode($raw, true);
  echo json_encode(['ok' => true, 'value' => $val]);
  exit;
}

if($action === 'set_blob'){
  $raw = file_get_contents('php://input');
  $body = json_decode($raw, true);
  if(!is_array($body)){
    echo json_encode(['ok' => false, 'error' => 'Invalid JSON']);
    exit;
  }
  $key = isset($body['key']) ? $body['key'] : '';
  if($key === ''){
    echo json_encode(['ok' => false, 'error' => 'Missing key']);
    exit;
  }
  $p = blob_path($baseDir, $key);
  if(!$p){
    echo json_encode(['ok' => false, 'error' => 'Bad key']);
    exit;
  }
  $backup = $baseDir . DIRECTORY_SEPARATOR . 'backup_blob_' . preg_replace('/[^a-z0-9_\-]/i','', $key) . '.json';
  $ok = write_blob($p, $backup, $body['value']);
  echo json_encode(['ok' => $ok]);
  exit;
}

if($action === 'set_full'){
  $raw = file_get_contents('php://input');
  $body = json_decode($raw, true);
  if(!is_array($body)){
    echo json_encode(['ok' => false, 'error' => 'Invalid JSON']);
    exit;
  }
  $db = $body;
  $db['_meta'] = ['updatedAt' => gmdate('c')];
  $ok = write_db($dbFile, $backupFile, $db);
  echo json_encode(['ok' => $ok]);
  exit;
}

if($action === 'get'){
  $key = isset($_GET['key']) ? $_GET['key'] : '';
  $db = read_db($dbFile);
  if($key === ''){
    echo json_encode(['ok' => true, 'value' => $db]);
    exit;
  }
  $val = array_key_exists($key, $db) ? $db[$key] : null;
  echo json_encode(['ok' => true, 'value' => $val]);
  exit;
}

if($action === 'set'){
  $raw = file_get_contents('php://input');
  $body = json_decode($raw, true);
  if(!is_array($body)){
    echo json_encode(['ok' => false, 'error' => 'Invalid JSON']);
    exit;
  }
  $key = isset($body['key']) ? $body['key'] : '';
  if($key === ''){
    echo json_encode(['ok' => false, 'error' => 'Missing key']);
    exit;
  }
  $db = read_db($dbFile);
  $db[$key] = $body['value'];
  $db['_meta'] = ['updatedAt' => gmdate('c')];
  $ok = write_db($dbFile, $backupFile, $db);
  echo json_encode(['ok' => $ok]);
  exit;
}

if($action === 'clear'){
  $db = ['_meta' => ['updatedAt' => gmdate('c')]];
  $ok = write_db($dbFile, $backupFile, $db);
  echo json_encode(['ok' => $ok]);
  exit;
}

echo json_encode(['ok' => false, 'error' => 'Unknown action']);
