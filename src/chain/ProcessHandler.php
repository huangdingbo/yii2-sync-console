<?php


namespace dsj\sync\console\chain;

use dsj\components\helpers\LogHelper;
use dsj\components\helpers\processes\AbsMultipleProcessesHandler;
use dsj\components\server\rules\RuleClient;
use dsj\sync\web\models\TSyncTask;
use yii\db\Connection;
use yii\helpers\Json;

/**
 * Class ProcessHandler
 * @package dsj\sync\console\chain
 * 开启多进程以后，进程间不能通信，所以传入的连接对象没有用
 */
class ProcessHandler extends AbsMultipleProcessesHandler
{
    private $pageSize = 100;

    public function execute()
    {
        $index = $this->process->getIndex();

        $tables = $this->params['processParams'][$index];

        /**
         * @var $data_obj TSyncTask
         */
        $data_obj = $this->params['data_obj'];

        $config = \Yii::$app->getComponents()['db'];
        unset($config['class']);
        $db = new Connection($config);
        $db->open();

        /**
         * @var $sourceDb Connection
         */
        $sourceDb = $this->getDb($db,$data_obj->source_db_id);


        /**
         * @var $aidDb Connection
         */
        $aidDb = $this->getDb($db,$data_obj->aid_db_id);


        $pidMap = $this->process->getPidMap();

        $pid = $pidMap[$index] . ',';

        (new LogHelper())->setFileName($data_obj->id . '.pid')->setRoute(__CLASS__)
            ->setIsFormat(false)
            ->setData($pid)
            ->write();

        $flog = 0;
        while (1){
            $pidStr = rtrim((new LogHelper())->setFileName($data_obj->id . '.pid')->getLog(), ",");
            $pidArr = explode(',',$pidStr);
            if (count($pidArr) == $data_obj->process_num){
                break;
            }
            if ($flog == 10){
                break;
            }
            $flog++;
        }

        $db->createCommand("UPDATE t_sync_task SET pid='{$pidStr}' WHERE id={$data_obj->id}")->execute();

        $executeRulesArr = Json::decode($data_obj->execute_rule,true);

        foreach ($tables as $table){

            $where = isset($executeRulesArr[$table]['where']) ?  $executeRulesArr[$table]['where'] : [];

            $sqlArr = $this->getSql($table,$where);
            $total = $sourceDb->createCommand($sqlArr['countSql'])->queryOne()['num'];
            $pages = ceil($total/$this->pageSize);
            for ($i = 0;$i < $pages;$i++){
                $offset = $i * $this->pageSize;
                $sourceData = $sourceDb->createCommand($sqlArr['querySql'] . " limit {$offset},{$this->pageSize}")->queryAll();
                $unique = isset($executeRulesArr[$table]['unique']) ? $executeRulesArr[$table]['unique'] : 'id';
                $this->insertOrUpdateAidDb($sourceData,$unique,$aidDb,$table);
            }

        }

        $sourceDb->close();

        $aidDb->close();

        $db->close();
    }

    /**
     * @param $db Connection
     * @param $id
     * @return Connection
     * @throws \yii\db\Exception
     */
    private function getDb($db,$id){
        $info = $db->createCommand("select * from t_sync_db where id={$id}")->queryOne();
        $connection = new \yii\db\Connection([
            'dsn' => "mysql:host={$info['host']};dbname={$info['db_name']};port={$info['port']}",
            'username' => "{$info['username']}",
            'password' => "{$info['password']}",
            'charset' => "{$info['connect_charset']}",
        ]);

        $connection->open();

        return $connection;
    }

    private function parseRules(&$rules){
        foreach ($rules as &$rule){
            if (is_array($rule)){
                $this->parseRules($rule);
            }else{
                $rule = RuleClient::decode($rule);
            }
        }
    }

    private function getSql($table,$where){

        $str = '';

        if ($where){
            $this->parseRules($where);

            foreach ($where as $item){
                if (count($item) == 1){
                    foreach ($item as $key=>$val){
                        $str = $str == '' ? "$key = '{$val}'" : "and $key = '{$val}'";
                    }
                }

                if (count($item) == 3){
                    $str = $str == '' ? "$item[1] $item[0] '{$item[2]}'" : "and $item[1] $item[0] '{$item[2]}'";
                }
            }
        }

        $querySql = "select * from {$table}";

        $countSql = "select count(*) num from {$table}";

        if ($str){
            $querySql = $querySql . " where {$str}";
            $countSql = $countSql . " where {$str}";
        }

        return [
            'querySql' => $querySql,
            'countSql' => $countSql
        ];
    }

    /**
     * @param $sourceData
     * @param $unique
     * @param $aidDb Connection
     */
    private function insertOrUpdateAidDb($sourceData,$unique,$aidDb,$table){
        foreach ($sourceData as $item){
            if ($unique == 'id'){
                $id = $item['id'];
                if ($aidDb->createCommand("select * from {$table} where id = {$id}")->queryOne()){
                    $aidDb->createCommand()->update($table,$item,['id' => $id])->execute();
                }else{
                    $aidDb->createCommand()->insert($table,$item)->execute();
                }
            }else{
                $arr = explode(',',$unique);
                $whereStr = '';
                $whereArr = [];
                foreach ($arr as $value){
                    $whereStr = $whereStr == '' ? " {$value} = '{$item[$value]}'" : $whereStr . " and {$value} = '{$item[$value]}'";
                    $whereArr[$value] = $item[$value];
                }

                if ($aidDb->createCommand("select * from {$table} where {$whereStr}")->queryOne()){
                    $aidDb->createCommand()->update($table,$item,$whereArr)->execute();
                }else{
                    $aidDb->createCommand()->insert($table,$item)->execute();
                }
            }
        }
    }

    public static function className()
    {
        return __CLASS__;
    }
}